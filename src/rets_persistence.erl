%%%-------------------------------------------------------------------
%%% @author redink
%%% @copyright (C) , redink
%%% @doc
%%%
%%% @end
%%% Created :  by redink
%%%-------------------------------------------------------------------
-module(rets_persistence).

-behaviour(gen_server).

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-callback handle_recover([{atom(), term()}]) -> {ok, map()}.
-callback handle_log_entry(tuple(), map()) -> {ok, map()}.
-callback handle_clean_log(map()) -> {ok, map()}.
-callback handle_delete_log(map()) -> ok.
-callback handle_terminate(map()) -> ok.

-define(HIBERNATE_TIMEOUT, 1000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------

start_link(Mod, Args, Options) ->
    gen_server:start_link(?MODULE, [Mod | Args], Options).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Mod | [Arg]]) ->
    {ok, State} = Mod:handle_recover(Arg),
    {ok, #{ modname => Mod
          , modstate => State}, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_call({aof, _, clean_aof_log, _}, _From,
            #{ modname  := Mod
             , modstate := ModState} = State) ->
    {ok, NewModState} = Mod:handle_clean_log(ModState),
    {reply, ok, State#{modstate := NewModState}, ?HIBERNATE_TIMEOUT};

handle_call({aof, _, delete_aof_log, _}, _From,
            #{ modname  := Mod
             , modstate := ModState} = State) ->
    ok = Mod:handle_delete_log(ModState),
    {reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({aof, _, close_aof_log, _}, _From, State) ->
    {stop, normal, ok, State};

handle_call({aof, _, _, _} = LogEntry, _From,
            #{ modname  := Mod
             , modstate := ModState} = State) ->
    {ok, NewModState} = Mod:handle_log_entry(LogEntry, ModState),
    {reply, ok, State#{modstate := NewModState}, ?HIBERNATE_TIMEOUT};

handle_call(_, _, State) ->
    {reply, unsupported, State, ?HIBERNATE_TIMEOUT}.

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
terminate(_Reason, #{modname := Mod, modstate := ModState}) ->
    ok = Mod:handle_terminate(ModState),
    ok.

%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

