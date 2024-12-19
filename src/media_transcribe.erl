%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2024, 2600Hz
%%% @doc Handles transcription from call recorded file
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(media_transcribe).

%% API
-export([request/2]).

-spec request(binary(), kz_term:proplist()) -> 'ok' | 'undefined'.
request(Attachment, Props) ->
    kz_log:put_callid(props:get_value('call_id', Props)),

    lager:info("start transcribing ~s from call recording with props ~p"
              ,[props:get_binary_value('attachment_id', Props), Props]
              ),

    Req = asr_request:from_media_recording(Attachment, Props),
    Req0 = asr_request:transcribe(Req),
    case asr_request:error(Req0) of
        'undefined' ->
            lager:info("successfully transcribed the call recording");
        Error ->
            lager:debug("error transcribing ~p", [Error]),
            'undefined'
    end.
