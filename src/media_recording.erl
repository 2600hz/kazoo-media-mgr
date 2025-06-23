%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2025, 2600Hz
%%% @doc Handles stop recording.
%%%
%%% @author Luis Azedo
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(media_recording).

-behaviour(gen_listener).

-export([start_link/0]).

-export([handle_record_stop/2]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,handle_event/2
        ,terminate/2
        ,code_change/3
        ]).

-include("media.hrl").

-define(SERVER, ?MODULE).

-type store_url() :: 'false' |
                     {'true', 'local'} |
                     {'true', 'other', kz_term:ne_binary()}.

-type state() :: #{queue => kz_term:ne_binary()
                  ,is_consuming => boolean()
                  }.

-type store_info() :: #{url := kz_term:ne_binary()
                       ,media := kz_recording:media()
                       ,extension := kz_term:ne_binary()
                       ,doc_id := kz_term:ne_binary()
                       ,doc_db := kz_term:ne_binary()
                       ,cdr_id := kz_term:api_ne_binary()
                       ,interaction_id := kz_term:ne_binary()
                       ,conference_name := kz_term:api_ne_binary()
                       ,should_store := store_url()
                       ,retries := non_neg_integer()
                       ,verb := kz_term:ne_binary()
                       ,account_id := kz_term:ne_binary()
                       ,endpoint_id := kz_term:ne_binary()
                       ,call_id := kz_term:api_ne_binary()
                       ,event := kz_call_event:payload()
                       ,origin := kz_term:ne_binary()
                       ,pid := pid()
                       ,data := kz_json:object()
                       ,user_metadata => kz_json:object()
                       ,transcribe := boolean()
                       }.

%% By convention, we put the options here in macros, but not required.
-define(BINDINGS, [{'call', [{'restrict_to', ['RECORD_STOP']}]}
                  ,{'conference', [{'restrict_to', [{'event', [{'event', <<"stop-recording">>}]}]}]}
                  ]).

-define(RESPONDERS, [{{?MODULE, 'handle_record_stop'}, [{<<"*">>, <<"*">>}]}]).

-define(QUEUE_NAME, <<"recordings">>).
-define(QUEUE_OPTIONS, [{'exclusive', 'false'}]).
-define(CONSUME_OPTIONS, [{'exclusive', 'false'}]).

-spec additional_kapi_bindings() -> kz_term:ne_binaries().
additional_kapi_bindings() ->
    kz_app_config:get_ne_binaries(?CONFIG_APP, <<"record_stop_additional_kapi_bindings">>, []).

-spec bindings() -> gen_listener:bindings().
bindings() ->
    [{kz_term:to_atom(KAPI, 'true'), [{'restrict_to', ['RECORD_STOP']}]} || KAPI <- additional_kapi_bindings()] ++ ?BINDINGS.

-spec start_link() -> kz_types:startlink_ret().
start_link() ->
    gen_listener:start_link(?SERVER
                           ,[{'bindings', bindings()}
                            ,{'responders', ?RESPONDERS}
                            ,{'queue_name', ?QUEUE_NAME}       % optional to include
                            ,{'queue_options', ?QUEUE_OPTIONS} % optional to include
                            ,{'consume_options', ?CONSUME_OPTIONS} % optional to include
                            ]
                           ,[]
                           ).

-spec handle_record_stop(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_record_stop(RecordStop, Props) ->
    Pid = props:get_value('server', Props),
    case kz_conference_event:event(RecordStop) of
        <<"stop-recording">> ->
            maybe_save_conference_recording(Pid, RecordStop);
        _ ->
            kz_log:put_callid(RecordStop),
            maybe_save_recording(Pid, RecordStop)
    end.

-spec init([]) -> {'ok', state()}.
init([]) ->
    lager:info("starting event listener for inbound endpoint record_call"),
    {'ok', #{}}.

%%------------------------------------------------------------------------------
%% @doc Handling call messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_call(any(), kz_term:pid_ref(), state()) -> kz_types:handle_call_ret_state(state()).
handle_call(_Request, _From, State) ->
    {'reply', {'error', 'not_implemented'}, State}.

%%------------------------------------------------------------------------------
%% @doc Handling cast messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_cast(any(), state()) -> kz_types:handle_cast_ret_state(state()).
handle_cast({'store_succeeded', #{}}, State) ->
    lager:debug("store succeeded"),
    {'noreply', State};

handle_cast({'store_failed', #{retries := 0} = Store, Error}, State) ->
    case ?RETRY_ENABLED of
        'true' ->
            lager:debug("store failed : ~p, saving locally as last resort.", [Error]),
            self() ! {'last_resort', Store};
        'false' -> lager:debug("store failed : ~p, no more retries.", [Error])
    end,
    {'noreply', State};

handle_cast({'store_failed', #{doc_db := Db ,doc_id := DocId}=_Store, 'file_not_found'}, State) ->
    _ = kz_datamgr:del_doc(Db, DocId),
    {'noreply', State};
handle_cast({'store_failed', #{retries := Retries} = Store, Error}, State) ->
    Sleep = ?MILLISECONDS_IN_MINUTE * rand:uniform(10),
    lager:debug("store failed : ~p, retrying ~p more times, next in ~p minute(s)"
               ,[Error, Retries, Sleep / ?MILLISECONDS_IN_MINUTE]
               ),
    _TRef = erlang:send_after(Sleep, self(), {'retry_storage', Store}),
    {'noreply', State};
handle_cast({'last_resort_failed', _Store, Error}, State) ->
    lager:debug("last resort failed : ~p, no more retries.", [Error]),
    {'noreply', State};
handle_cast({'last_resort_succeeded', #{}}, State) ->
    lager:debug("last resort succeeded"),
    {'noreply', State};
handle_cast({'gen_listener',{'created_queue', Queue}}, State) ->
    {'noreply', State#{queue => Queue}};
handle_cast({'gen_listener',{'is_consuming', 'true'}}, State) ->
    lager:debug("we're ready to accept recording events"),
    {'noreply', State#{is_consuming => 'true'}};
handle_cast({'gen_listener',{'is_consuming', 'false'}}, State) ->
    lager:warning("we're not consuming any events"),
    {'noreply', State#{is_consuming => 'false'}};

handle_cast(_Msg, State) ->
    lager:debug("unhandled cast: ~p", [_Msg]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc Handling all non call/cast messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_info(any(), state()) -> kz_types:handle_info_ret_state(state()).
handle_info({'retry_storage', #{retries := Retries} = Store}, State) ->
    _ = kz_process:spawn(fun() -> save_recording(Store#{retries => Retries - 1}) end),
    {'noreply', State};

handle_info({'last_resort', Store}, State) ->
    _ = kz_process:spawn(fun() -> recording_last_resort(Store) end),
    {'noreply', State};

handle_info(_Info, State) ->
    lager:debug("unhandled message: ~p", [_Info]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc Allows listener to pass options to handlers.
%% @end
%%------------------------------------------------------------------------------
-spec handle_event(kz_json:object(), kz_term:proplist()) -> gen_listener:handle_event_return().
handle_event(_JObj, _State) ->
    {'reply', []}.

%%------------------------------------------------------------------------------
%% @doc This function is called by a `gen_server' when it is about to
%% terminate. It should be the opposite of `Module:init/1' and do any
%% necessary cleaning up. When it returns, the `gen_server' terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%------------------------------------------------------------------------------
-spec terminate(any(), state()) -> 'ok'.
terminate(_Reason, _State) ->
    lager:debug("listener terminating: ~p", [_Reason]).

%%------------------------------------------------------------------------------
%% @doc Convert process state when code is changed.
%% @end
%%------------------------------------------------------------------------------
-spec code_change(any(), state(), any()) -> {'ok', state()}.
code_change(_OldVsn, State, _Extra) ->
    {'ok', State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Get and store the recording metadata in an MODb.
%% @end
%%------------------------------------------------------------------------------
-spec store_recording_meta(store_info()) -> kz_term:ne_binary() | {'error', any()}.
store_recording_meta(#{doc_db := Db
                      ,doc_id := DocId
                      ,account_id := AccountId
                      }=StoreInfo) ->
    BaseMediaDoc = recording_meta(StoreInfo),
    MediaDoc = kz_doc:update_pvt_parameters(BaseMediaDoc, Db, [{'type', kzd_call_recordings:type()}]),
    case kz_datamgr:save_doc(Db, MediaDoc, [{'ensure_saved', 'true'}]) of
        {'ok', Doc} ->
            lager:debug("recording meta ~s saved for ~s", [DocId, AccountId]),
            kz_doc:revision(Doc);
        {'error', _}= Err -> Err
    end.

%%------------------------------------------------------------------------------
%% @doc Get the recording metadata to be stored from the store info.
%% @end
%%------------------------------------------------------------------------------
-spec recording_meta(store_info()) -> kzd_call_recordings:doc().
recording_meta(#{media := {_, MediaName}
                ,doc_id := DocId
                ,cdr_id := CdrId
                ,interaction_id := InteractionId
                ,conference_name := ConferenceName
                ,url := Url
                ,call_id := CallId
                ,event := JObj
                ,account_id := AccountId
                ,endpoint_id := EndpointId
                ,origin := Origin
                ,data := Data
                ,transcribe := Transcribe
                }=StoreInfo) ->
    Ext = filename:extension(MediaName),
    LengthMs = recording_length(JObj),
    LengthSeconds = LengthMs div ?MILLISECONDS_IN_SECOND,
    Start = record_start_time(JObj, LengthSeconds),

    ConferenceId = kz_conference_event:conference_id(JObj),
    IsConference = kz_term:is_not_empty(ConferenceId),

    BaseMediaDoc = kz_json:from_list(
                     [{<<"_id">>, DocId}
                     ,{<<"call_id">>, CallId}
                     ,{<<"callee_id_name">>, kz_call_event:callee_id_name(JObj)}
                     ,{<<"callee_id_number">>, kz_call_event:callee_id_number(JObj)}
                     ,{<<"caller_id_name">>, kz_call_event:caller_id_name(JObj)}
                     ,{<<"caller_id_number">>, kz_call_event:caller_id_number(JObj)}
                     ,{<<"cdr_id">>, CdrId}
                     ,{<<"content_type">>, kz_mime:from_extension(Ext)}
                     ,{<<"custom_channel_vars">>, kz_call_event:custom_channel_vars(JObj)}
                     ,{<<"description">>, recording_description(MediaName, IsConference)}
                     ,{<<"direction">>, kz_call_event:call_direction(JObj)}
                     ,{<<"duration">>, LengthSeconds}
                     ,{<<"duration_ms">>, LengthMs}
                     ,{<<"account_id">>, meta_account_id(AccountId, kz_ccv:account_id(JObj))}
                     ,{<<"endpoint_id">>, EndpointId}
                     ,{<<"from">>, kz_json:get_ne_binary_value(<<"From">>, JObj)}
                     ,{<<"interaction_id">>, InteractionId}
                     ,{<<"media_source">>, <<"recorded">>}
                     ,{<<"media_type">>, Ext}
                     ,{<<"name">>, MediaName}
                     ,{<<"origin">>, Origin}
                     ,{<<"owner_id">>, kz_call_event:custom_channel_var(JObj, <<"Owner-ID">>)}
                     ,{<<"request">>, kz_json:get_ne_binary_value(<<"Request">>, JObj)}
                     ,{<<"source_type">>, kz_term:to_binary(?MODULE)}
                     ,{<<"start">>, Start}
                     ,{<<"to">>, kz_json:get_ne_binary_value(<<"To">>, JObj)}
                     ,{<<"url">>, Url}
                     ,{<<"recording_vars">>, recording_vars(Data)}
                     ,{<<"conference_recording">>, IsConference}
                     ,{<<"conference_id">>, ConferenceId}
                     ,{<<"conference_name">>, ConferenceName}
                     ,{<<"transcribe">>, Transcribe}
                     ]
                    ),
    maybe_set_user_metadata(StoreInfo, BaseMediaDoc).

-spec recording_description(kz_term:ne_binary(), boolean()) -> kz_term:ne_binary().
recording_description(MediaName, 'false') ->
    <<"recording ", MediaName/binary>>;
recording_description(MediaName, 'true') ->
    <<"conference recording ", MediaName/binary>>.

-spec meta_account_id(kz_term:api_ne_binary(), kz_term:api_ne_binary()) -> kz_term:api_ne_binary().
meta_account_id(AccountId, AccountId) -> 'undefined';
meta_account_id(_AccountId, AccountId) -> AccountId.

-spec recording_vars(kz_json:object()) -> kz_term:api_object().
recording_vars(Data) ->
    case kz_json:get_json_value(<<"recording_vars">>, Data) of
        'undefined' -> 'undefined';
        JSON -> kz_json:normalize(JSON)
    end.

%%------------------------------------------------------------------------------
%% @doc Set the user metadata on the recording metadata if the user metadata has
%% been defined.
%% @end
%%------------------------------------------------------------------------------
-spec maybe_set_user_metadata(store_info(), kzd_call_recordings:doc()) -> kzd_call_recordings:doc().
maybe_set_user_metadata(#{user_metadata := UserMetadata}, Doc) ->
    kzd_call_recordings:set_user_metadata(Doc, UserMetadata);
maybe_set_user_metadata(_, Doc) -> Doc.

-spec recording_length(kz_json:object()) -> integer().
recording_length(JObj) ->
    case kz_call_event:recording_length(JObj) of
        'undefined' -> 0;
        Length -> Length
    end.

-spec record_start_time(kz_json:object(), kz_term:api_integer()) -> integer().
record_start_time(JObj, Length) ->
    case kz_recording:record_start_time_us(JObj) of
        'undefined' ->
            case kz_call_event:timestamp(JObj) of
                'undefined' ->
                    kz_time:now_s() - Length;
                Timestamp ->
                    Timestamp - Length
            end;
        StartUs ->
            kz_time:microseconds_to_seconds(
              kz_time:unix_us_to_gregorian_us(StartUs)
             )
    end.

-spec maybe_store_recording_meta(store_info()) -> kz_term:ne_binary() | {'error', any()}.
maybe_store_recording_meta(#{doc_db := Db
                            ,doc_id := DocId
                            }=State) ->
    case kz_datamgr:lookup_doc_rev(Db, {kzd_call_recordings:type(), DocId}) of
        {'ok', Rev} -> Rev;
        _ -> store_recording_meta(State)
    end.

-spec store_url(store_info(), kz_term:ne_binary()) -> kz_term:ne_binary().
store_url(#{doc_db := Db
           ,doc_id := MediaId
           ,media := {_, MediaName}
           ,should_store := {'true', 'local'}
           ,transcribe := Transcribe
           } = Store, _Rev) ->
    media_url:store(Db, {kzd_call_recordings:type(), MediaId}, MediaName, get_transcribe_option(Transcribe, Store));
store_url(#{doc_db := Db
           ,doc_id := MediaId
           ,media := {_, MediaName}
           ,should_store := {'true', 'other', Url}
           ,verb := Verb
           ,transcribe := Transcribe
           } = Store, _Rev) ->
    HandlerOpts = #{url => Url
                   ,verb => Verb
                   ,field_separator => <<>>
                   ,field_list => handler_fields(Url, Store)
                   },
    AttHandler = kapps_call_recording:handler_from_url(Url),
    Handler = #{att_proxy => 'true'
               ,att_post_handler => 'external'
               ,att_handler => {AttHandler, HandlerOpts}
               },
    Options = [{'plan_override', Handler}] ++ get_transcribe_option(Transcribe, Store),
    media_url:store(Db, {kzd_call_recordings:type(), MediaId}, MediaName, Options).

-spec handler_fields(kz_term:ne_binary(), store_info()) ->
          kz_term:proplist().
handler_fields(Url, Store) ->
    {Protocol, _, _, _, _} = kz_http_util:urlsplit(Url),
    handler_fields_for_protocol(Protocol, Url, Store).

-spec handler_fields_for_protocol(kz_term:ne_binary(), kz_term:ne_binary(), store_info()) ->
          kz_term:proplist().
handler_fields_for_protocol(<<"ftp", _/binary>>, _Url, #{'extension':=Ext}) ->
    [{'const', <<"call_recording_">>}
    ,{'field', <<"call_id">>}
    ,{'const', get_extension(Ext)}
    ];
handler_fields_for_protocol(<<"sftp", _/binary>>, _Url, #{'extension':=Ext}) ->
    [{'const', <<"call_recording_">>}
    ,{'field', <<"call_id">>}
    ,{'const', get_extension(Ext)}
    ];
handler_fields_for_protocol(<<"http", _/binary>>, Url, #{'account_id':=AccountId
                                                        ,'extension':=Ext
                                                        ,'doc_id':=DocId
                                                        ,'event':=Event
                                                        }) ->
    {S1, S2} = check_url(Url),
    [{'const', <<S1/binary, "call_recording_">>}, {'field', <<"call_id">>}, {'const', get_extension(Ext)}
    ,{'const', <<S2/binary, "from=">>}, {'field', <<"from">>}
    ,{'const', <<"&to=">>}, {'field', <<"to">>}
    ,{'const', <<"&caller_id_name=">>}, {'field', <<"caller_id_name">>}
    ,{'const', <<"&caller_id_number=">>}, {'field', <<"caller_id_number">>}
    ,{'const', <<"&call_id=">>}, {'field', <<"call_id">>}
    ,{'const', <<"&cdr_id=">>}, {'field', <<"cdr_id">>}
    ,{'const', <<"&interaction_id=">>}, {'field', <<"interaction_id">>}
    ,{'const', <<"&account_id=">>}, AccountId
    ,{'const', <<"&duration_ms=">>}, {'field', <<"duration_ms">>}
    ,{'const', <<"&owner_id=">>}, {'field', <<"owner_id">>}
    ,{'const', <<"&start=">>}, {'field', <<"start">>}
    ,{'const', <<"&recording_id=">>}, {'const', DocId}
    | maybe_add_conference_queries(Event)
    ].

-spec get_extension(kz_term:ne_binary()) -> kz_term:ne_binary().
get_extension(<<".", _/binary>> = Ext) -> Ext;
get_extension(Ext) -> <<".", Ext/binary>>.

-spec check_url(kz_term:ne_binary()) -> {binary(), kz_term:ne_binary()}.
check_url(Url) ->
    case kz_http_util:urlsplit(Url) of
        {_, _, _, <<>>, _} -> {<<>>, <<"?">>};
        {_, _, _, Params, _} -> {check_url_query(Params), <<"&">>}
    end.

-spec check_url_query(kz_term:ne_binary()) -> binary().
check_url_query(Query) ->
    check_url_param(lists:last(binary:split(Query, <<"&">>, ['global']))).

-spec check_url_param(kz_term:ne_binary()) -> binary().
check_url_param(Param) ->
    case binary:split(Param, <<"=">>) of
        [_] -> <<"=">>;
        [_, <<>>] -> <<>>;
        _ -> <<"&recording=">>
    end.

-spec save_recording(store_info()) -> 'ok'.
save_recording(#{media := {_, MediaName}, should_store := 'false'}) ->
    lager:info("not configured to store recording ~s", [MediaName]);
save_recording(#{media := Media, pid := Pid}=Store) ->
    case maybe_store_recording_meta(Store) of
        {'error', Err} ->
            lager:warning("error storing metadata : ~p", [Err]),
            gen_server:cast(Pid, {'store_failed', Store});
        Rev ->
            StoreUrl = fun()-> store_url(Store, Rev) end,
            store_recording(Media, StoreUrl, Store)
    end.

-spec recording_last_resort(store_info()) -> 'ok'.
recording_last_resort(#{media := {_, MediaName}, should_store := 'false'}) ->
    lager:info("not configured to store recording ~s", [MediaName]);
recording_last_resort(#{event := JObj, media := Media, pid := Pid}=Store) ->
    case maybe_store_recording_meta(Store) of
        {'error', Err} ->
            lager:warning("error storing metadata : ~p", [Err]),
            gen_server:cast(Pid, {'store_failed', Store});
        Rev ->
            StoreUrl = fun()->
                               Url = store_url(Store, Rev),
                               <<Url/binary,"?last_resort=true">>
                       end,
            Node = kz_call_event:switch_nodename(JObj),
            {DirName, MediaName} = Media,
            Filename = filename:join(DirName, MediaName),
            case kz_storage:store_file(Node, Filename, StoreUrl, Store) of
                {'error', Error} -> gen_server:cast(Pid, {'last_resort_failed', Store, Error});
                'ok' -> gen_server:cast(Pid, {'last_resort_succeeded', Store})
            end
    end.

-spec store_recording(kz_recording:media(), kz_term:ne_binary() | function(), store_info()) -> 'ok'.
store_recording({DirName, MediaName}, StoreUrl, #{event := JObj, pid := Pid} = Map) ->
    Node = kz_call_event:switch_nodename(JObj),
    Filename = filename:join(DirName, MediaName),
    case kz_storage:store_file(Node, Filename, StoreUrl, Map) of
        {'error', Error} -> gen_server:cast(Pid, {'store_failed', Map, Error});
        'ok' -> gen_server:cast(Pid, {'store_succeeded', Map})
    end.

-spec maybe_save_recording(pid(), kz_json:object()) -> 'ok'.
maybe_save_recording(Pid, RecordStop) ->
    case is_channel_loopback_bowout(RecordStop) of
        'false' ->
            maybe_save_recording(Pid, RecordStop, kz_recording:recorder(RecordStop));
        'true' ->
            lager:info("recording not stored since it was done in a loopback bowout leg")
    end.

-spec is_channel_loopback_bowout(kz_json:object()) -> boolean().
is_channel_loopback_bowout(RecordStop) ->
    kz_json:is_true(<<"Channel-Is-Loopback">>, RecordStop)
        andalso kz_json:is_true(<<"Channel-Loopback-Bowout">>, RecordStop).

-spec maybe_save_recording(pid(), kz_json:object(), kz_term:api_ne_binary()) -> 'ok'.
maybe_save_recording(Pid, JObj, ?KZ_RECORDER) ->
    AccountId = kz_recording:account_id(JObj, kz_json:get_binary_value(<<"Account-ID">>, JObj)),
    Media = {_, MediaId} = kz_recording:response_media(JObj),
    Ext = filename:extension(MediaId),
    lager:debug("saving recording media ~s in account ~s", [MediaId, AccountId]),
    Data = kz_recording:data(JObj, kz_json:new()),
    DocId = kz_recording:id(JObj),

    {Year, Month, _} = erlang:date(),
    DefaultDB = kazoo_modb:get_modb(AccountId, Year, Month),
    DocDb = recording_db(Data, DefaultDB),
    CallId = kz_call_event:call_id(JObj),
    CdrId = cdr_id(CallId, Year, Month),
    InteractionId = interaction_id(JObj),
    ConferenceName = kz_json:get_ne_binary_value(<<"Conference-Name">>, Data),
    Url = kz_json:get_ne_binary_value(<<"url">>, Data),
    Transcribe = kz_json:is_true(<<"transcribe">>, Data),
    ShouldStore = should_store_recording(Data, AccountId, Url),
    Verb = kz_json:get_ne_binary_value(<<"method">>, Data, <<"put">>),
    Origin = kz_json:get_ne_binary_value(<<"origin">>, Data, <<"no origin">>),
    EndpointId = kz_json:get_ne_binary_value(<<"endpoint_id">>, Data),
    Retries = storage_retry_times(Data, AccountId),

    Store = #{url => Url
             ,media => Media
             ,extension => Ext
             ,doc_id => DocId
             ,doc_db => DocDb
             ,cdr_id => CdrId
             ,interaction_id => InteractionId
             ,conference_name => ConferenceName
             ,should_store => ShouldStore
             ,retries => Retries
             ,verb => Verb
             ,account_id => AccountId
             ,endpoint_id => EndpointId
             ,call_id => CallId
             ,event => JObj
             ,origin => Origin
             ,pid => Pid
             ,data => Data
             ,transcribe => Transcribe
             },
    save_recording(maybe_set_default_user_metadata(Store));
maybe_save_recording(_Pid, _JObj, _Recorder) ->
    lager:info("recorder ~s not handled", [_Recorder]).

-spec interaction_id(kz_json:object()) -> kz_term:ne_binary().
interaction_id(JObj) ->
    case kz_conference_event:conference_vars(JObj) of
        'undefined' ->
            kz_call_event:custom_channel_var(JObj, <<?CALL_INTERACTION_ID>>);
        ConfVars ->
            kz_json:get_ne_binary_value(<<"Interaction-ID">>, ConfVars)
    end.

-spec recording_db(kz_json:object(), kz_term:ne_binary()) -> kz_term:ne_binary().
recording_db(Data, DefaultDB) ->
    kz_json:get_ne_binary_value(<<"media_db">>, Data, DefaultDB).

-spec should_store_recording(kz_json:object(), kz_term:api_ne_binary(), kz_term:api_ne_binary()) ->
          kapps_call_recording:store_url().
should_store_recording(_Data, 'undefined', _Url) -> 'false';
should_store_recording(Data, AccountId, Url) ->
    case kz_json:get_boolean_value(<<"should_store_recording">>, Data) of
        'undefined' -> kapps_call_recording:should_store_recording(AccountId, Url);
        'true' -> {'true', 'local'};
        'false' -> 'false'
    end.

-spec storage_retry_times(kz_json:object(), kz_term:api_ne_binary()) -> pos_integer().
storage_retry_times(Data, AccountId) ->
    case kz_json:get_integer_value(<<"storage_retry_times">>, Data) of
        'undefined' -> media_config:storage_retry_times(AccountId);
        Value when Value > 0 -> Value;
        _Other -> media_config:storage_retry_times(AccountId)
    end.

%%------------------------------------------------------------------------------
%% @doc Set the default user metadata on the store info if some default metadata
%% has been configured.
%% @end
%%------------------------------------------------------------------------------
-spec maybe_set_default_user_metadata(store_info()) -> store_info().
maybe_set_default_user_metadata(#{account_id := AccountId}=StoreInfo) ->
    case kapps_account_config:get_with_strategy(<<"global">>
                                               ,AccountId
                                               ,?CONFIG_CAT
                                               ,[<<"call_recording">>, <<"default_user_metadata">>]
                                               )
    of
        'undefined' -> StoreInfo;
        DefaultUserMetadata -> StoreInfo#{user_metadata => DefaultUserMetadata}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% conference media recording storing
%% @end
%%------------------------------------------------------------------------------
-spec maybe_save_conference_recording(pid(), kz_term:object()) -> 'ok'.
maybe_save_conference_recording(Pid, JObj) ->
    ConferenceId = kz_json:get_binary_value(<<"Conference-ID">>, JObj),
    kz_log:put_callid(ConferenceId),
    case kz_recording:data(JObj) of
        'undefined' ->
            lager:info("not handle conference record because of missing data");
        Data ->
            Recorder = kz_json:get_binary_value(<<"Recorder">>, Data),

            Recording0 = kz_json:get_value(<<"Recording">>, JObj),
            %% freeswicth may add recording info to path (probably conference had video enabled):
            %% {video_time_audio=false,channels=2,samplerate=16000,vw=1280,vh=720,fps=15.00}/tmp/filename.ext
            FilePath = re:replace(kz_json:get_ne_binary_value(<<"File-Path">>, Recording0)
                                 ,<<"\\{[^}]+\\}">>
                                 ,<<>>
                                 ,[{'return', 'binary'}]
                                 ),

            Prop = [{<<"Account-ID">>, kz_conference_event:account_id(JObj)}
                   ,{<<"File-Path">>, FilePath}
                   ],
            Recording = kz_json:merge(Recording0, kz_json:set_values(Prop, Data)),
            maybe_save_recording(Pid, kz_json:set_value(<<"Recording">>, Recording, JObj), Recorder)
    end.

-spec maybe_add_conference_queries(kz_json:object()) -> kz_term:proplist().
maybe_add_conference_queries(Event) ->
    ConferenceId = kz_conference_event:conference_id(Event),
    case kz_term:is_not_empty(ConferenceId) of
        'false' -> [];
        'true' ->
            [{'const', <<"&conference_recording=">>}, {'field', <<"conference_recording">>}
            ,{'const', <<"&conference_id=">>}, {'field', <<"conference_id">>}
            ]
    end.

cdr_id('undefined', _Year, _Month) ->
    'undefined';
cdr_id(CallId, Year, Month) ->
    ?MATCH_MODB_PREFIX(kz_term:to_binary(Year), kz_date:pad_month(Month), CallId).

-spec get_transcribe_option(boolean(), store_info()) -> kz_term:proplist().
get_transcribe_option('false', _Store) ->
    [];
get_transcribe_option('true', #{media := {_, MediaId}
                               ,doc_db := Db
                               ,doc_id := DocId
                               ,account_id := AccountId
                               ,call_id := CallId
                               ,extension := Ext
                               ,event := JObj
                               }) ->
    TranscribeInfo = [{'account_id', AccountId}
                     ,{'call_id', CallId}
                     ,{'content_type', kz_mime:from_extension(Ext)}
                     ,{'recording_milliseconds', recording_length(JObj)}
                     ,{'media_id', DocId}
                     ,{'attachment_id', MediaId}
                     ,{'modb', Db}
                     ],
    [{'transcribe', TranscribeInfo}].
