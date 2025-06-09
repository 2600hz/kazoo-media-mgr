-ifndef(MEDIA_HRL).

-include_lib("kazoo_stdlib/include/kz_types.hrl").
-include_lib("kazoo_stdlib/include/kz_log.hrl").
-include_lib("kazoo_stdlib/include/kz_databases.hrl").

-define(APP, media_srv).
-define(CONFIG_APP, media).
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
                    ,{<<"proxy_store_retry_enabled">>, 'false'}
                    ,{<<"proxy_store_retry_max_parallel">>, 10}
                    ,{<<"proxy_store_retry_scan_period_s">>, 600}
                    ,{<<"proxy_store_retry_attempts">>, 1}
                    ,{<<"proxy_store_retry_method">>, <<"local file">>}
                    ,{<<"proxy_store_retry_tmp_dir">>, <<"/tmp">>}
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

-define(RETRY_ENABLED, kapps_config:get_boolean(?CONFIG_CAT, <<"proxy_store_retry_enabled">>, 'false')).
-define(RETRY_MAX_PARALLEL, kapps_config:get_integer(?CONFIG_CAT, <<"proxy_store_retry_max_parallel">>, 10)).
-define(RETRY_SCAN_PERIOD, kapps_config:get_integer(?CONFIG_CAT, <<"proxy_store_retry_scan_period_s">>, ?SECONDS_IN_MINUTE * 10) * ?MILLISECONDS_IN_SECOND).
-define(RETRY_ATTEMPTS, kapps_config:get_integer(?CONFIG_CAT, <<"proxy_store_retry_attempts">>, 1)).
-define(RETRY_METHOD, kapps_config:get_binary(?CONFIG_CAT, <<"proxy_store_retry_method">>, <<"local file">>)).
-define(RETRY_TMPDIR, kapps_config:get_binary(?CONFIG_CAT, <<"proxy_store_retry_tmp_dir">>, <<"/tmp">>)).

-define(MEDIA_HRL, 'true').
-endif.
