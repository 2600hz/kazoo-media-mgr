%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2010-2025, 2600Hz
%%% @doc
%%% @author James Aimonetti
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(media_mgr_sup).

-behaviour(supervisor).

-include_lib("kazoo_stdlib/include/kz_types.hrl").

-define(SERVER, ?MODULE).

-export([start_link/0]).
-export([init/1]).

-define(CHILDREN, [?SUPER('media_recording_sup')
                  ,?SUPER('media_cache_sup')
                  ,?WORKER_ARGS('kazoo_etsmgr_srv'
                               ,[
                                 [{'table_id', media_map:table_id()}
                                 ,{'table_options', media_map:table_options()}
                                 ,{'find_me_function', fun media_map:find_me_function/0}
                                 ,{'gift_data', media_map:gift_data()}
                                 ]
                                ])
                  ,?WORKER('media_map')
                  ,?WORKER('media_proxy')
                  ,?WORKER('media_listener')
                  ,?WORKER('media_proxy_retry_worker')
                  ]).

%%==============================================================================
%% API functions
%%==============================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the supervisor.
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> kz_types:startlink_ret().
start_link() ->
    supervisor:start_link({'local', ?SERVER}, ?MODULE, []).

%%==============================================================================
%% Supervisor callbacks
%%==============================================================================

%%------------------------------------------------------------------------------
%% @doc Whenever a supervisor is started using `supervisor:start_link/[2,3]',
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%% @end
%%------------------------------------------------------------------------------
-spec init(any()) -> kz_types:sup_init_ret().
init([]) ->
    _ = kz_process:set_startup(),
    RestartStrategy = 'one_for_one',
    MaxRestarts = 5,
    MaxSecondsBetweenRestarts = 10,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    {'ok', {SupFlags, ?CHILDREN}}.
