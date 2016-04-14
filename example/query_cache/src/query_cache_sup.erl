-module(query_cache_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    QueryCacheInstance =
        {query_cache_instance_sup,
         {query_cache_tmp_sup, start_link, [query_cache_instance_sup,
                                            query_cache_instance]},
         permanent, 5000, supervisor, [query_cache_tmp_sup]},
    {ok, { {one_for_one, 5, 10}, [QueryCacheInstance]} }.

