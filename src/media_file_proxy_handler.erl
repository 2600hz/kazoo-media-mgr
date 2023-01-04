%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2010-2023, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(media_file_proxy_handler).
-behaviour(cowboy_handler).

-export([init/2
        ,terminate/3
        ]).

-include("media.hrl").

-spec init(cowboy_req:req(), map()) -> {'ok', cowboy_req:req(), map() | 'ok'}.
init(Req, #{name := Name, type := StreamType} = Context) ->
    lager:info("starting ~s media file proxy for ~s", [StreamType, Name]),
    Routines = [fun decode_req/1
               ,fun fetch_doc/1
               ,fun meta/1
               ,fun fetch_stream/1
               ,fun start_stream/1
               ,fun stream/1
               ],
    run(Context#{req => Req}, Routines).

run(#{req := Req}, []) -> {'ok', Req, 'ok'};
run(Context, [Fun | Funs]) ->
    case Fun(Context) of
        NewContext when is_map(NewContext) -> run(NewContext, Funs);
        Reply when is_tuple(Reply) -> Reply
    end.

decode_req(#{url := Url} = Context) ->
    {Db, Id, Attachment, Options} = binary_to_term(base64:decode(Url)),
    Context#{db => Db
            ,id => Id
            ,attachment => Attachment
            ,options => Options
            }.

fetch_doc(#{req := Req, db := Db, id := Id, attachment := Attachment} = Context) ->
    lager:debug("fetching ~s/~s/~s", [Db, Id, Attachment]),
    case kz_datamgr:open_doc(Db, Id) of
        {'ok', JObj} ->
            Context#{doc => JObj};
        {'error', 'not_found'} ->
            lager:warning("failed to find '~s' in '~s'", [Id, Db]),
            {'ok', cowboy_req:reply(404, Req), 'ok'};
        {'error', Reason} ->
            lager:debug("unable get metadata for ~s on ~s in ~s: ~p", [Attachment, Id, Db, Reason]),
            {'ok', cowboy_req:reply(404, Req), 'ok'}
    end.

meta(#{req := Req, doc := JObj, attachment := Attachment} = Context) ->
    case kz_doc:attachment(JObj, Attachment) of
        undefined -> {'ok', cowboy_req:reply(404, Req), 'ok'};
        Meta -> Context#{meta => Meta}
    end.

fetch_stream(#{req := Req, db := Db, id := Id, attachment := Attachment} = Context) ->
    case kz_datamgr:stream_attachment(Db, Id, Attachment) of
        {'ok', Ref} -> Context#{stream_ref => Ref};
        _Else -> {'ok', cowboy_req:reply(404, Req), 'ok'}
    end.

start_stream(#{req := Req, meta := Meta} = Context) ->
    ContentType = kz_json:get_ne_binary_value(<<"content_type">>, Meta),
    Headers = resp_headers(ContentType),
    Context#{req => cowboy_req:stream_reply(200, Headers, Req)}.

stream(#{req := Req, stream_ref := Ref} = Context) ->
    receive
        {Ref, done} ->
            cowboy_req:stream_body(<<>>, 'fin', Req),
            {'ok', Req, 'ok'};
        {Ref, {'ok', Bin}} ->
            cowboy_req:stream_body(Bin, 'nofin', Req),
            stream(Context);
        {Ref, {'error', _E}} ->
            {'ok', cowboy_req:reply(503, Req), 'ok'}
    end.


-spec resp_headers(kz_term:ne_binary()) -> cowboy:http_headers().
resp_headers(ContentType) ->
    #{<<"content-type">> => ContentType
     ,<<"server">> => list_to_binary([?APP_NAME, "/", ?APP_VERSION])
     }.

-spec terminate(any(), cowboy_req:req(), any()) -> 'ok'.
terminate(_Reason, _Req, _State) ->
    lager:debug("terminating media file proxy req: ~p", [_Reason]).
