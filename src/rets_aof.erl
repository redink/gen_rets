%%%-------------------------------------------------------------------
%%% @author redink
%%% @copyright (C) , redink
%%% @doc
%%%
%%% @end
%%% Created :  by redink
%%%-------------------------------------------------------------------
-module(rets_aof).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(HIBERNATE_TIMEOUT, hibernate).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #{}, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    {reply, ok, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_info(timeout, State) ->
    proc_lib:hibernate(gen_server, enter_loop,
               [?MODULE, [], State]),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(_Info, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

