-ifndef(MEDIA_HRL).

-include_lib("kazoo_stdlib/include/kz_types.hrl").
-include_lib("kazoo_stdlib/include/kz_log.hrl").
-include_lib("kazoo_stdlib/include/kz_databases.hrl").

-define(APP_NAME, <<"media_srv">>).
-define(APP_VERSION, <<"4.0.0">>).
-define(CONFIG_CAT, <<"media">>).

-define(MEDIA_DB, <<"system_media">>).

-define(PROMPT_LANGUAGE_KEY, <<"default_language">>).

-define(CONFIG_KVS, [{<<"use_https">>, 'false'}
                    ,{<<"authenticated_playback">>, 'false'}
                    ,{<<"authenticated_store">>, 'true'}
                    ,{<<"proxy_store_authenticate">>, 'true'}
                    ,{<<"proxy_username">>, 'undefined'}
                    ,{<<"proxy_password">>, 'undefined'}
                    ,{<<"proxy_store_acls">>, [<<"127.0.0.0/24">>]}
                    ,{<<"max_recording_time_limit">>, media_util:max_recording_time_limit()}
                    ,{[<<"call_recording">>, <<"extension">>], <<"mp3">>}
                    ,{<<"store_recordings">>, 'false'}
                    ,{<<"third_party_bigcouch_host">>, 'undefined'}
                    ,{<<"third_party_bigcouch_port">>, 5984}
                    ,{<<"use_bigcouch_direct">>, 'true'}
                    ,{<<"bigcouch_host">>, 'undefined'}
                    ,{<<"bigcouch_port">>, 'undefined'}
                    ,{<<"use_media_proxy">>, 'true'}
                    ,{<<"proxy_port">>, 24517}
                    ,{<<"use_plaintext">>, 'true'}
                    ,{<<"proxy_listeners">>, 25}
                    ,{<<"use_ssl_proxy">>, 'false'}
                    ,{<<"ssl_cert">>, 'undefined'}
                    ,{<<"ssl_key">>, 'undefined'}
                    ,{<<"ssl_port">>, 'undefined'}
                    ,{<<"ssl_password">>, 'undefined'}
                    ,{<<"record_min_sec">>, 0}
                    ]).

-define(CHUNKSIZE, 24576).

-type media_store_option() :: {'content-type', kz_term:ne_binary()} |
                              {'doc_type', kz_term:ne_binary()} |
                              {'rev', kz_term:ne_binary()} |
                              {'plan_override', map()} |
                              {'storage_id', kz_term:ne_binary()} |
                              {'doc_owner', kz_term:ne_binary()} |
                              {'save_error', boolean()} |
                              {'error_verbosity', 'verbose'}.
-type media_store_options() :: [media_store_option()].

-record(media_store_path, {db :: kz_term:ne_binary()
                          ,id :: kz_term:ne_binary()
                          ,att :: kz_term:ne_binary()
                          ,opt = [] :: media_store_options()
                          }).

-type media_store_path() :: #media_store_path{}.

-define(MEDIA_HRL, 'true').
-endif.
