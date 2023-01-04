%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2023, 2600Hz
%%% @doc
%%% @author Karl Anderson
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(media_url).

-export([playback/1, playback/2]).
-export([store/2, store/3, store/4]).

-include("media.hrl").

-define(STREAM_TYPE_STORE, kz_json:from_list([{<<"Stream-Type">>, <<"store">>}])).

-type build_media_url() :: kz_term:api_binary() | kz_term:binaries() | kz_json:object().
-type build_media_url_ret() :: kz_term:ne_binary() | {'error', atom()}.


-spec playback(build_media_url()) -> build_media_url_ret().
playback('undefined') ->
    {'error', 'invalid_media_name'};
playback(Arg) ->
    playback(Arg, kz_json:new()).

-spec playback(build_media_url(), kz_json:object()) -> build_media_url_ret().
playback('undefined', _) ->
    {'error', 'invalid_media_name'};
playback(<<"tts://", Id/binary>>, Options) ->
    lager:debug("lookup tts media url for ~s", [Id]),
    media_tts:get_uri(Id, Options);
playback(<<"prompt://", PromptPath/binary>>, Options) ->
    lager:debug("looking up prompt path ~s", [PromptPath]),
    case binary:split(PromptPath, <<"/">>, ['global']) of
        [AccountId, PromptId, Language] ->
            Media = media_map:prompt_path(AccountId, PromptId, Language),
            playback(Media, Options);
        [AccountId, <<(PromptId):32/binary>>] ->
            lager:info("got req for media id ~s in ~s", [PromptId, AccountId]),
            playback(PromptPath, Options);
        [PromptId, <<Language:2/binary>>] ->
            lager:info("got req for prompt ~s for language ~s, taking from ~s", [PromptId, Language, ?KZ_MEDIA_DB]),
            Media = media_map:prompt_path(?KZ_MEDIA_DB, PromptId, Language),
            playback(Media, Options);
        [PromptId, <<_Lang:2/binary, "-", _Region:2/binary>> = Language] ->
            lager:info("got req for prompt ~s for language ~s, taking from ~s", [PromptId, Language, ?KZ_MEDIA_DB]),
            Media = media_map:prompt_path(?KZ_MEDIA_DB, PromptId, Language),
            playback(Media, Options);
        [AccountId, PromptId] ->
            lager:info("got req for prompt ~s without language, checking account ~s", [PromptId, AccountId]),
            Language = media_util:prompt_language(AccountId),
            Media = media_map:prompt_path(AccountId, PromptId, Language),
            playback(Media, Options);
        [<<(PromptId):32/binary>>] ->
            lager:info("got req for ~s without account, checking with ~s", [PromptId, ?KZ_MEDIA_DB]),
            playback(list_to_binary([?KZ_MEDIA_DB, "/", PromptId]), Options);
        [PromptId] ->
            lager:info("got req for prompt ~s without account and language, checking with ~s", [PromptId, ?KZ_MEDIA_DB]),
            Language = media_util:prompt_language(?KZ_MEDIA_DB),
            Media = media_map:prompt_path(?KZ_MEDIA_DB, PromptId, Language),
            playback(Media, Options)
    end;
playback(<<Media/binary>>, JObj) ->
    lager:debug("lookup media url for ~s", [Media]),
    media_file:get_uri(Media, JObj);
playback(Path, JObj)
  when is_list(Path) ->
    media_file:get_uri(Path, JObj);
playback(Doc, JObj) ->
    lager:debug("building media url from doc"),
    case media_util:store_path_from_doc(Doc) of
        #media_store_path{}=Media -> media_file:get_uri(Media, JObj);
        Error -> Error
    end.

-spec store(kz_json:object(), kz_term:ne_binary()) ->
          build_media_url_ret().
store(JObj, AName) ->
    Media = media_util:store_path_from_doc(JObj, AName),
    media_file:get_uri(Media, ?STREAM_TYPE_STORE).

-spec store(kz_term:ne_binary(), kazoo_data:docid(), kz_term:ne_binary()) ->
          build_media_url_ret().
store(Db, Id, Attachment) ->
    store(Db, Id, Attachment, []).

-spec store(kz_term:ne_binary(), kazoo_data:docid(), kz_term:ne_binary(), kz_term:proplist()) ->
          build_media_url_ret().
store(Db, {Type, Id}, Attachment, Options) ->
    store(Db, Id, Attachment, [{'doc_type', Type} | Options]);
store(Db, ?NE_BINARY = Id, Attachment, Options) ->
    media_file:get_uri([Db, Id, Attachment, Options], ?STREAM_TYPE_STORE).
