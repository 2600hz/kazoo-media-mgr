%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2025, 2600Hz
%%% @doc
%%% @author Evgeny Noskov
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(media_proxy_retry_worker).
-behaviour(gen_server).

-export([start_link/0]).
-export([load_jobs/1]).
-export([status/0]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3
        ]).

-include("media.hrl").

-define(SERVER, ?MODULE).

-record(job, {doc_id :: kz_term:api_ne_binary()     % equals to media_doc._id from modb
             ,modb_id :: kz_term:api_ne_binary()    % modb which hosts the media doc
             ,attachment :: kz_term:api_ne_binary() % filename like 6e806453ecd0d34f8665a04143b28d81.mp3
             ,filepath :: file:filename_all()   % absolute filepath like "/my_tmp/modb_docid_name.mp3"
             ,retry_count = 0 :: non_neg_integer()  % number of retries
             }).
-type job() :: #job{}.
-type jobs() :: [job()].

-record(state, {running = #{} :: #{kz_term:ne_binary() => {job(), pid(), reference()}}
               ,active_jobs = 0 :: non_neg_integer()
               ,max_parallel = ?RETRY_MAX_PARALLEL :: pos_integer()
               ,success = 0 :: non_neg_integer()
               ,failed = 0 :: non_neg_integer()
               ,pending = [] :: jobs()
               ,scanner_pid_ref :: kz_term:api_pid_ref()
               }).
-type state() :: #state{}.

-define(NUM_OF_RETRIES, ?RETRY_ATTEMPTS - 1). % 1 initial attempt followed by N retries

-define(AUDIO_REGEX, "^([^_]+)_([^_]+)_([^\\.]+)\\.(mp3|wav|ogg|flac|aac|m4a|wma)$").

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the server.
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> kz_types:startlink_ret().
start_link() ->
    gen_server:start_link({'local', ?SERVER}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc % Manually trigger a scan (optional)
%% @end
%%------------------------------------------------------------------------------
-spec load_jobs(kz_term:ne_binary()) -> 'ok'.
load_jobs(Dir) ->
    gen_server:cast(?MODULE, {'scan_jobs', Dir}).

%%------------------------------------------------------------------------------
%% @doc Get statistics
%% @end
%%------------------------------------------------------------------------------
-spec status() -> #{success := non_neg_integer()
                   ,failed := non_neg_integer()
                   ,active_jobs => non_neg_integer()
                   ,active_doc_ids => kz_term:ne_binaries()
                   }.
status() ->
    gen_server:call(?SERVER, 'status').

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Initializes the server.
%% @end
%%------------------------------------------------------------------------------
-spec init([]) -> {'ok', state()}.
init([]) ->
    kz_log:put_callid(?MODULE),
    lager:debug("started ~s", [?MODULE]),
    {'ok', PidRef} = spawn_scanner(?RETRY_SCAN_PERIOD),
    {'ok', #state{max_parallel = ?RETRY_MAX_PARALLEL
                 ,scanner_pid_ref = PidRef
                 }}.

%%------------------------------------------------------------------------------
%% @doc Handling call messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_call(any(), kz_term:pid_ref(), state()) -> kz_types:handle_call_ret_state(state()).
handle_call('status', _From, #state{success = S, failed = F, active_jobs = A, running = Running} = State) ->
    {'reply', {'ok', #{success => S, failed => F, active_jobs => A, active_doc_ids => maps:keys(Running)}}, State};
handle_call(_Request, _From, State) ->
    lager:debug("unhandled call: ~p", [_Request]),
    {'reply', {'error', 'not_implemented'}, State}.

%%------------------------------------------------------------------------------
%% @doc Handling cast messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_cast(any(), state()) -> kz_types:handle_cast_ret_state(state()).
handle_cast({'scan_jobs', Dir}, State) ->
    _ = kz_process:spawn_monitor(fun scan_jobs/1, [Dir]),
    {'noreply', State};
handle_cast({'discovered_jobs', Jobs}, State) ->
    {'noreply', schedule_jobs(Jobs, State)};
handle_cast(_Msg, State) ->
    lager:debug("unhandled cast: ~p", [_Msg]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc Handling all non call/cast messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_info(any(), state()) -> kz_types:handle_info_ret_state(state()).
handle_info({'DOWN', Ref, 'process', Pid, Reason}, State) ->
    {'noreply', handle_down_message(Pid, Ref, Reason, State)};
handle_info(_Info, State) ->
    lager:debug("unhandled message: ~p", [_Info]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc This function is called by a `gen_server' when it is about to
%% terminate. It should be the opposite of `Module:init/1' and do any
%% necessary cleaning up. When it returns, the `gen_server' terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%------------------------------------------------------------------------------
-spec terminate(any(), state()) -> 'ok'.
terminate(Reason, #state{scanner_pid_ref = {Pid, _Ref}}) ->
    Pid ! 'stop',
    lager:info("media proxy retry worker ~p terminated with reason : ~p", [self(), Reason]).

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
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec spawn_scanner(pos_integer()) -> {'ok', kz_term:pid_ref()}.
spawn_scanner(Interval) ->
    {'ok', kz_process:spawn_monitor(fun scanner_loop/1, [Interval])}.

-spec scanner_loop(pos_integer()) -> 'ok'.
scanner_loop(Interval) ->
    receive
        'stop' -> 'ok'
    after Interval ->
            scan_jobs(?RETRY_TMPDIR),
            scanner_loop(Interval)
    end.

-spec scan_jobs(kz_term:ne_binary()) -> 'ok'.
scan_jobs(Dir) ->
    case get_retry_jobs(Dir) of
        [] ->
            lager:debug("scanning completed with no new jobs"),
            'ok';
        Jobs ->
            gen_server:cast(?SERVER, {'discovered_jobs', Jobs})
    end.

-spec get_retry_jobs(kz_term:ne_binary()) -> jobs().
get_retry_jobs(Dir) ->
    get_retry_jobs(Dir, ?RETRY_METHOD).

-spec get_retry_jobs(kz_term:ne_binary(), kz_term:ne_binary()) -> jobs().
get_retry_jobs(Dir, <<"local file">>) ->
    {'ok', Regex} = re:compile(?AUDIO_REGEX, ['caseless']),
    Wildcard = filename:join([Dir, "*_*_*.*"]), % /tmp/{MODB}_{DOCID}_{Name.mp3}
    Candidates = filelib:wildcard(kz_term:to_list(Wildcard)),
    [get_retry_job_from_file(File) || File <- Candidates, does_file_match(File, Regex)];
get_retry_jobs(_Map, _Method) ->
    lager:warning("media proxy retry method ~p is not supported", [_Method]).

-spec does_file_match(file:filename_all(), re:mp()) -> boolean().
does_file_match(File, Regex) ->
    case re:run(File, Regex) of
        {'match', _} -> 'true';
        'nomatch' -> 'false'
    end.

-spec get_retry_job_from_file(file:filename_all()) -> job().
get_retry_job_from_file(File) ->
    Filename = kz_term:to_binary(filename:basename(File)),
    [MoDb, DocId, Attachment] = binary:split(Filename, <<"_">>, ['global']),
    lager:debug("queueing new media retry job ~s for ~s(~s)"
               ,[Attachment
                ,DocId
                ,kzs_util:format_account_modb(MoDb, 'encoded')
                ]),
    #job{doc_id = DocId
        ,attachment = Attachment
        ,modb_id = MoDb
        ,filepath = File
        ,retry_count = 0
        }.

-spec schedule_jobs(jobs(), state()) -> state().
schedule_jobs(Jobs, State) ->
    lists:foldl(fun schedule_job/2, State, Jobs).

schedule_job(Job, #state{running = Running} = State) ->
    #job{doc_id = DocId} = Job,
    case maps:is_key(DocId, Running) of
        'true' -> State;  % Job already running
        'false' -> start_job_if_possible(Job, State)
    end.

-spec start_job_if_possible(job(), state()) -> state().
start_job_if_possible(#job{doc_id = DocId} = Job
                     ,#state{active_jobs = Active, max_parallel = Max} = State
                     )
  when Active < Max ->
    {Pid, Ref} = kz_process:spawn_monitor(fun process_job/1, [Job]),
    State#state{running = maps:put(DocId, {Job, Pid, Ref}, State#state.running)
               ,active_jobs = Active + 1
               };
start_job_if_possible(Job, #state{pending = Pending} = State) ->
    State#state{pending = [Job | Pending]}.

-spec process_job(job()) -> no_return().
process_job(Job) ->
    case retry_media(Job) of
        'ok' ->
            exit('normal');
        {'error', Reason} ->
            maybe_retry_job(Job, Reason)
    end.

-spec maybe_retry_job(job(), any()) -> no_return().
maybe_retry_job(#job{retry_count = Retry}, Reason) ->
    case Retry < ?NUM_OF_RETRIES of
        'true' -> exit({'retry', Reason});
        _ -> exit({'failed', Reason})
    end.

-spec retry_media(job()) -> 'ok' | {'error', kz_term:ne_binary()}.
retry_media(Job) ->
    retry_media(Job, ?RETRY_METHOD).

-spec retry_media(job(), kz_term:ne_binary()) -> 'ok' | {'error', kz_term:ne_binary()}.
retry_media(#job{modb_id=MODBId
                ,doc_id=DocId
                ,attachment=Attachment
                ,filepath=Filepath
                }
           ,<<"local file">>
           ) ->
    MoDbEncoded = kzs_util:format_account_modb(MODBId, 'encoded'),
    case file:read_file(Filepath) of
        {'ok', Contents} ->
            MimeType = get_mime_type(Filepath),
            case kz_datamgr:put_attachment(MoDbEncoded, DocId, Attachment, Contents, [{'content-type', MimeType}]) of
                {'ok', _} ->
                    lager:info("[OK] successfully retried media ~s for ~s(~s)", [Attachment, DocId, MoDbEncoded]),
                    cleanup_file(Filepath),
                    'ok';
                {'ok', _, _} ->
                    lager:info("[OK] successfully retried media ~s for ~s(~s)", [Attachment, DocId, MoDbEncoded]),
                    cleanup_file(Filepath),
                    'ok';
                {'error', _Fail} ->
                    lager:warning("[AGAIN] failed to upload media ~s for ~s(~s)", [Attachment, DocId, MoDbEncoded]),
                    {'error', <<"failed to put attachment">>}
            end;
        {'error', _E} ->
            lager:error("| ~s | ~s | ~s file ~s failed to open ~p~n", [MoDbEncoded, DocId, Attachment, Filepath, _E]),
            {'error', <<"failed to read file">>}
    end;
retry_media(_Job, _Method) ->
    lager:warning("media proxy retry method ~p is not supported", [_Method]).

-spec get_mime_type(kz_term:ne_binary()) -> kz_term:ne_binary().
get_mime_type(Filename) ->
    Ext = filename:extension(Filename),
    case kz_term:is_empty(Ext) of
        'true' -> <<"audio/mpeg">>;
        _ -> kz_mime:from_extension(Ext)
    end.

-spec cleanup_file(kz_term:api_ne_binary()) -> 'ok'.
cleanup_file(File) ->
    case file:delete(File) of
        'ok' -> lager:info("deleted file ~s~n", [File]);
        {'error', _E} -> lager:error("error deleting file ~s: ~p~n", [File, _E])
    end.

-spec handle_down_message(pid(), reference(), any(), state()) -> state().
handle_down_message(Pid, Ref, Reason, #state{running = Running, active_jobs = Active} = State) ->
    case find_job_by_pid(Pid, Running) of
        {'found', DocId, Job, Ref} ->
            NewRunning = maps:remove(DocId, Running),
            State1 = State#state{running = NewRunning, active_jobs = Active - 1},
            State2 = handle_job_completion(Reason, Job, State1),
            maybe_start_pending_jobs(State2);
        'not_found' ->
            case State#state.scanner_pid_ref of
                {Pid, Ref} ->
                    %% Scanner died, restart it
                    {'ok', NewPidRef} = spawn_scanner(?RETRY_SCAN_PERIOD),
                    State#state{scanner_pid_ref = NewPidRef};
                _ ->
                    State
            end;
        _ -> State
    end.

-spec find_job_by_pid(pid(), #{kz_term:ne_binary() => {job(), pid(), reference()}}) ->
          'not_found' | {'found', kz_term:ne_binary(), job(), reference()}.
find_job_by_pid(Pid, Running) ->
    Iterator = maps:iterator(Running),
    find_job_by_pid_iter(Pid, maps:next(Iterator)).

find_job_by_pid_iter(_Pid, 'none') -> 'not_found';
find_job_by_pid_iter(Pid, {DocId, {Job, Pid, Ref}, _Iterator}) ->
    {'found', DocId, Job, Ref};
find_job_by_pid_iter(Pid, {_, _, Iterator}) ->
    find_job_by_pid_iter(Pid, maps:next(Iterator)).

-spec handle_job_completion(any(), job(), state()) -> state().
handle_job_completion('normal', _Job, #state{success = S} = State) ->
    State#state{success = S + 1};
handle_job_completion({'retry', _}, Job, #state{} = State) ->
    gen_server:cast(?SERVER, {'discovered_jobs', [Job#job{retry_count = Job#job.retry_count + 1}]}),
    State;
handle_job_completion(_, _Job, #state{failed = F} = State) ->
    State#state{failed = F + 1}.

-spec maybe_start_pending_jobs(state()) -> state().
maybe_start_pending_jobs(#state{pending = []} = State) ->
    State;
maybe_start_pending_jobs(#state{pending = [Job|Rest]} = State) ->
    start_job_if_possible(Job, State#state{pending = Rest}).
