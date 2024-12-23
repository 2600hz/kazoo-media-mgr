%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2025, 2600Hz
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
-export([make_request/2]).

-define(ASR_APP_CONFIG, <<"speech">>).
-define(ASR_TRIM_MEDIA_ENABLED
       ,kapps_config:get_boolean(?ASR_APP_CONFIG, <<"asr_trim_media_enabled">>, 'false')
       ).

-spec make_request(binary(), kz_term:proplist()) -> 'ok' | {'error', term()}.
make_request(Attachment, Props) ->
    kz_log:put_callid(props:get_value('call_id', Props)),
    case ?ASR_TRIM_MEDIA_ENABLED of
        'true' ->
            request_with_trim_option(Attachment, Props);
        'false' ->
            request(Attachment, Props)
    end.

-spec request_with_trim_option(binary(), kz_term:proplist()) -> 'ok' | {'error', term()}.
request_with_trim_option(Attachment, Props) ->
    AttachmentId = list_to_binary([kz_binary:rand_hex(16), "_", props:get_binary_value('attachment_id', Props)]),
    NewProps = props:set_value({'attachment_id', AttachmentId}, Props),
    request(Attachment, NewProps).

-spec request(binary(), kz_term:proplist()) -> 'ok'.
request(Attachment, Props) ->
    lager:info("start transcribing ~s from call recording with props ~p"
              ,[props:get_binary_value('attachment_id', Props), Props]
              ),

    Req = asr_request:from_media_recording(Attachment, Props),
    Req0 = asr_request:transcribe(Req),
    case asr_request:error(Req0) of
        'undefined' ->
            lager:info("successfully transcribed the call recording");
        Error ->
            lager:debug("error transcribing ~p", [Error])
    end.
