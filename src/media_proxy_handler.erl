%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2010-2023, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(media_proxy_handler).
-behaviour(cowboy_handler).

-export([init/2
        ,terminate/3
        ]).

-include("media.hrl").

-spec init(cowboy_req:req(), any()) -> {atom(), cowboy_req:req(), any()}.
init(Req, [StreamType]) ->
    kz_log:put_callid(kz_binary:rand_hex(16)),
    lager:info("starting ~s media proxy", [StreamType]),
    case cowboy_req:path_info(Req) of
        [<<"tts">>, Id] -> media_tts_proxy_handler:init(Req, #{type => StreamType, id => Id});
        [Url, Name] -> media_file_proxy_handler:init(Req, #{type => StreamType, url => Url, name => Name})
    end.

-spec terminate(any(), cowboy_req:req(), any()) -> 'ok'.
terminate(_Reason, _Req, _State) ->
    lager:debug("terminating proxy req: ~p", [_Reason]).
