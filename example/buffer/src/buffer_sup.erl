-module(buffer_sup).

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
    BufferInstance =
        {buffer_instance_sup,
         {buffer_tmp_sup, start_link, [buffer_instance_sup,
                                       buffer_instance]},
         permanent, 5000, supervisor, [buffer_tmp_sup]},
    {ok, { {one_for_one, 5, 10}, [BufferInstance]} }.

