%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2010-2023, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(media_tts_proxy_handler).
-behaviour(cowboy_handler).

-export([init/2
        ,terminate/3
        ]).

-include("media.hrl").

-define(STATE(Metadata, MediaBinary)
       ,{Metadata, MediaBinary}
       ).
-type state() :: ?STATE(kz_json:object(), binary()).
-type stream_type() :: 'single' | 'continuous'.

-spec init(cowboy_req:req(), map()) -> {'ok', cowboy_req:req(), state() | 'ok'}.
init(Req, #{id := Id, type := StreamType}) ->
    TTSId = filename:rootname(Id),
    lager:debug("fetching tts/~s", [TTSId]),
    try media_cache_sup:find_tts_server(TTSId) of
        {'ok', Pid} ->
            {Meta, Bin} = media_data(Pid, StreamType),
            handle(Req, ?STATE(Meta, Bin));
        {'error', _E} ->
            lager:debug("missing tts server for ~s: ~p", [TTSId, _E]),
            Req1 = cowboy_req:reply(404, Req),
            {'ok', Req1, 'ok'}
    catch
        _E:_R ->
            lager:debug("exception thrown: ~s: ~p", [_E, _R]),
            Req1 = cowboy_req:reply(404, Req),
            {'ok', Req1, 'ok'}
    end.

-spec handle(cowboy_req:req(), state()) -> {'ok', cowboy_req:req(), 'ok'}.
handle(Req0, ?STATE(Meta, Bin)) ->
    ContentType = kz_json:get_ne_binary_value(<<"content_type">>, Meta),
    Req1 = start_stream(Req0, Meta, Bin, ContentType),
    lager:debug("sent reply"),
    {'ok', Req1, 'ok'}.

start_stream(Req, Meta, Bin, ContentType)
  when ContentType =:= <<"audio/mpeg">>
       orelse ContentType =:= <<"audio/mp3">> ->
    Size = byte_size(Bin),
    ChunkSize = min(Size, ?CHUNKSIZE),
    MediaName = kz_json:get_binary_value(<<"media_name">>, Meta, <<>>),
    Url = kz_json:get_binary_value(<<"url">>, Meta, <<>>),

    lager:debug("media: ~s content-type: ~s size: ~b", [MediaName, ContentType, Size]),

    Req1 = cowboy_req:stream_reply(200
                                  ,media_proxy_util:resp_headers(ChunkSize, ContentType, MediaName, Url)
                                  ,Req
                                  ),

    ShoutHeader = media_proxy_util:get_shout_header(MediaName, Url),

    media_proxy_util:stream_body(Req1, ChunkSize, Bin, ShoutHeader, 'true'),
    Req1;
start_stream(Req, Meta, Bin, ContentType) ->
    Size = byte_size(Bin),
    ChunkSize = min(Size, ?CHUNKSIZE),

    lager:debug("media: ~s content-type: ~s size: ~b"
               ,[kz_json:get_binary_value(<<"media_name">>, Meta, <<>>), ContentType, Size]
               ),
    Req1 = cowboy_req:stream_reply(200, media_proxy_util:resp_headers(ContentType), Req),
    media_proxy_util:stream_body(Req1, ChunkSize, Bin, 'undefined', 'false'),
    Req1.

-spec terminate(any(), cowboy_req:req(), state()) -> 'ok'.
terminate(_Reason, _Req, _State) ->
    lager:debug("terminating proxy req: ~p", [_Reason]).

-spec media_data(pid(), stream_type()) -> {kz_json:object(), binary()}.
media_data(Pid, Function) ->
    media_tts_cache:Function(Pid).
