%%%-------------------------------------------------------------------
%%% @author redink
%%% @copyright (C) , redink
%%% @doc
%%%
%%% @end
%%% Created :  by redink
%%%-------------------------------------------------------------------
-module(gen_rets).

-behaviour(gen_server).

%% API
-export([ start_link/1
        , get_ets/1
        , new/3
        , insert/3
        , insert_new/3
        , delete/1
        , delete/2
        , delete_object/2
        , delete_all_objects/1
        , lookup/2
        , get_ttl/2
        , reset_ttl/3
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-define(SERVER, ?MODULE).
-define(HIBERNATE_TIMEOUT, 10000).

% -callback

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
-spec new(pid() | atom(), atom(), list()) -> ets:tid() | atom().
new(ServerPid, Name, Options) ->
    gen_server:call(ServerPid, {new, Name, Options}).

-spec get_ets(pid() | atom()) -> ets:tid() | atom().
get_ets(ServerPid) ->
    gen_server:call(ServerPid, get_ets).

-spec insert(pid() | atom(), tuple() | [tuple()],
             {sec, integer()} |
             {min, integer()} |
             {hour, integer()}
            ) -> boolean().
insert(ServerPid, Objects, TTLOption) ->
    gen_server:call(ServerPid, {insert, Objects, TTLOption}).

-spec insert_new(pid() | atom(), tuple() | [tuple()],
                 {sec, integer()} |
                 {min, integer()} |
                 {hour, integer()}
                ) -> boolean().
insert_new(ServerPid, Objects, TTLOption) ->
    gen_server:call(ServerPid, {insert_new, Objects, TTLOption}).

-spec delete(pid() | atom()) -> true.
delete(ServerPid) ->
    gen_server:call(ServerPid, delete).

-spec delete(pid() | atom(), term()) -> true.
delete(ServerPid, Key) ->
    gen_server:call(ServerPid, {delete, Key}).

-spec delete_object(pid() | atom(), tuple()) -> true.
delete_object(ServerPid, Object) ->
    gen_server:call(ServerPid, {delete_object, Object}).

-spec delete_all_objects(pid() | atom()) -> true.
delete_all_objects(ServerPid) ->
    gen_server:call(ServerPid, delete_all_objects).

-spec lookup(pid() | atom(), term()) -> [tuple()].
lookup(ServerPid, Key) ->
    gen_server:call(ServerPid, {lookup, Key}).

-spec get_ttl(pid() | atom(), term()) -> integer().
get_ttl(ServerPid, Key) ->
    gen_server:call(ServerPid, {get_ttl, Key}).

-spec reset_ttl(pid() | atom(), term(),
                {sec, integer()} |
                {min, integer()} |
                {hour, integer()}) -> -1 | true.
reset_ttl(ServerPid, Key, TTLOption) ->
    gen_server:call(ServerPid, {reset_ttl, Key, TTLOption}).

start_link(Options) ->
    gen_server:start_link(?MODULE, [Options], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Options]) ->
    ExpireMode    = get_list_item(expire_mode   , Options, fifo),
    MaxSize       = get_list_item(max_size      , Options, 100000),
    HighWaterSize = get_list_item(highwater_size, Options, 70000),
    MaxMem = get_mem_limit(get_list_item(max_mem, Options, {gb, 2})),
    HighWaterMem = get_mem_limit(get_list_item(highwater_mem, Options, {gb, 1.5})),
    erlang:send_after(timer:seconds(10), erlang:self(), ttl_clean),
    {ok, #{ expire_mode => ExpireMode
          , max_mem     => MaxMem
          , max_size    => MaxSize
          , highwater_mem  => HighWaterMem
          , highwater_size => HighWaterSize
          }, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_call({new, Name, Options}, _From, State) ->
    MetaTable = ets:new(ets_meta_table, [public, set, compressed]),
    TTLTable  = ets:new(ets_ttl_table , [public, ordered_set, compressed]),
    FIFOTable = ets:new(ets_fifo_table, [public, ordered_set, compressed]),
    LRUTable  = ets:new(ets_lru_table , [public, ordered_set, compressed]),
    MainTable = ets:new(Name, Options),
    {reply, MainTable,
     State#{ ets_main_table  => MainTable
           , ets_meta_table  => MetaTable
           , ets_ttl_table   => TTLTable
           , ets_fifo_table  => FIFOTable
           , ets_lru_table   => LRUTable
           , new_ets_options => Options
           },
     ?HIBERNATE_TIMEOUT};

handle_call(get_ets, _From, #{ets_main_table := MainTable} = State) ->
    {reply, MainTable, State, ?HIBERNATE_TIMEOUT};

handle_call({insert, Objects, TTLOption}, _From,
            #{ets_main_table := MainTable} = State) ->
    true = ets:insert(MainTable, Objects),
    ok   = set_ttl(Objects, State, TTLOption),
    erlang:send(erlang:self(), refresh_main_table),
    {reply, true, State, ?HIBERNATE_TIMEOUT};

handle_call({insert_new, Objects, TTLOption}, _From,
            #{ets_main_table := MainTable} = State) ->
    case ets:insert_new(MainTable, Objects) of
        true ->
            ok = set_ttl(Objects, State, TTLOption),
            erlang:send(erlang:self(), refresh_main_table),
            {reply, true, State, ?HIBERNATE_TIMEOUT};
        false ->
            {reply, false, State, ?HIBERNATE_TIMEOUT}
    end;

handle_call(delete, _From, State) ->
    {stop, normal, true, State};

handle_call({delete, Key}, _From,
            #{ets_main_table := MainTable} = State) ->
    case ets:lookup(MainTable, Key) of
        [] ->
            ignore;
        _ ->
            ok   = clean_other_table_via_key(Key, State),
            true = ets:delete(MainTable, Key)
    end,
    {reply, true, State, ?HIBERNATE_TIMEOUT};

handle_call({delete_object, Object}, _From,
            #{ets_main_table := MainTable,
              new_ets_options := NewETSOptions} = State) ->
    Key = get_object_key(NewETSOptions, Object),
    case ets:lookup(MainTable, Key) of
        [] ->
            ignore;
        [_] ->
            ok = clean_other_table_via_key(Key, State),
            ets:delete_object(MainTable, Object);
        [_ | _] ->
            ets:delete_object(MainTable, Object)
    end,
    {reply, true, State, ?HIBERNATE_TIMEOUT};

handle_call(delete_all_objects, _From,
            #{ets_main_table := MainTable,
              ets_meta_table := MetaTable,
              ets_ttl_table  := TTLTable ,
              ets_fifo_table := FIFOTable,
              ets_lru_table  := LRUTable  } = State) ->
    true = ets:delete_all_objects(MetaTable),
    true = ets:delete_all_objects(TTLTable ),
    true = ets:delete_all_objects(FIFOTable),
    true = ets:delete_all_objects(LRUTable ),
    true = ets:delete_all_objects(MainTable),
    {reply, true, State, ?HIBERNATE_TIMEOUT};

handle_call({lookup, Key}, _From,
            #{ets_meta_table := MetaTable,
              ets_main_table := MainTable} = State) ->
    case ets:lookup(MetaTable, Key) of
        [] ->
            {reply, ets:lookup(MainTable, Key), State,
             ?HIBERNATE_TIMEOUT};
        [{Key, TTLTime, _, _}] ->
            Now = get_now(),
            Return =
                case Now > TTLTime of
                    true ->
                        clean_other_table_via_key(Key, State), 
                        true = ets:delete(MainTable, Key),
                        [];
                    _ ->
                        update_lru_time(Now, Key, State),
                        ets:lookup(MainTable, Key)
                end,
            {reply, Return, State, ?HIBERNATE_TIMEOUT}
    end;

handle_call({get_ttl, Key}, _From,
            #{ets_meta_table := MetaTable} = State) ->
    case ets:lookup(MetaTable, Key) of
        [] ->
            {reply, -1, State, ?HIBERNATE_TIMEOUT};
        [{Key, TTLTime, _, _}] ->
            Now = get_now(),
            ok  = update_lru_time(Now, Key, State),
            {reply, TTLTime - Now, State, ?HIBERNATE_TIMEOUT}
    end;

handle_call({reset_ttl, Key, Time}, _From,
            #{ets_meta_table := MetaTable,
              ets_ttl_table  := TTLTable ,
              ets_lru_table  := LRUTable  } = State) ->
    case ets:lookup(MetaTable, Key) of
        [] ->
            {reply, -1, State, ?HIBERNATE_TIMEOUT};
        [{Key, OldTTLTime, OldInsertTime, OldUpdateTime}] ->
            Now     = get_now(),
            TTLTime = get_ttl_time(Time, Now),
            true = ets:delete(TTLTable, {OldTTLTime, Key}),
            true = ets:delete(LRUTable, {OldUpdateTime, Key}),
            true = ets:insert(TTLTable, {{TTLTime, Key}, nouse}),
            true = ets:insert(LRUTable, {{Now, Key}, nouse}),
            true = ets:insert(MetaTable, {Key, TTLTime, OldInsertTime, Now}),
            {reply, true, State, ?HIBERNATE_TIMEOUT}
    end;

handle_call(_Request, _From, State) ->
    {reply, unsupported, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------

handle_info(ttl_clean, State) ->
    case maps:get(ets_main_table, State, -1) of
        -1 ->
            ignore;
        _ ->
            ok = clean_unuse_via_ttl(State),
            erlang:send_after(timer:seconds(10), erlang:self(), ttl_clean)
    end,
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(refresh_main_table, State) ->
    erlang:send(erlang:self(), refresh_main_table_via_size),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(refresh_main_table_via_size, #{ expire_mode := ExpireMode
                                          , max_size    := MaxSize
                                          , highwater_size := HighWaterSize
                                          , ets_main_table := MainTable
                                          } = State) ->
    case {ets:info(MainTable, size) > MaxSize, ExpireMode} of
        {true, lru} ->
            clean_single_key_via_lru(State),
            erlang:send(self(), {refresh_main_table_via_size, HighWaterSize});
        {true, fifo} ->
            clean_single_key_via_fifo(State),
            erlang:send(self(), {refresh_main_table_via_size, HighWaterSize});
        _ ->
            erlang:send(self(), refresh_main_table_via_mem)
    end,
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(refresh_main_table_via_mem, #{ expire_mode := ExpireMode
                                         , max_mem     := MaxMem
                                         , highwater_mem := HighWaterMem
                                         , ets_main_table := MainTable
                                         } = State) ->
    case {ets:info(MainTable, memory) > MaxMem, ExpireMode} of
        {true, lru} ->
            clean_single_key_via_lru(State),
            erlang:send(self(), {refresh_main_table_via_mem, HighWaterMem});
        {true, fifo} ->
            clean_single_key_via_fifo(State),
            erlang:send(self(), {refresh_main_table_via_mem, HighWaterMem});
        _ ->
            ignore
    end,
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({refresh_main_table_via_size, HighWaterSize},
            #{expire_mode := ExpireMode, ets_main_table := MainTable} = State) ->
    case {ets:info(MainTable, size) > HighWaterSize, ExpireMode} of
        {true, lru} ->
            clean_single_key_via_lru(State),
            erlang:send(self(), {refresh_main_table_via_size, HighWaterSize});
        {true, fifo} ->
            clean_single_key_via_fifo(State),
            erlang:send(self(), {refresh_main_table_via_size, HighWaterSize});
        _ ->
            erlang:send(self(), refresh_main_table_via_mem)
    end,
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({refresh_main_table_via_mem, HighWaterMem},
            #{expire_mode := ExpireMode, ets_main_table := MainTable} = State) ->
    case {ets:info(MainTable, memory) > HighWaterMem, ExpireMode} of
        {true, lru} ->
            clean_single_key_via_lru(State),
            erlang:send(self(), {refresh_main_table_via_mem, HighWaterMem});
        {true, fifo} ->
            clean_single_key_via_fifo(State),
            erlang:send(self(), {refresh_main_table_via_mem, HighWaterMem});
        _ ->
            ignore
    end,
    {noreply, State, ?HIBERNATE_TIMEOUT};

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

get_mem_limit({gb, Num}) ->
    erlang:trunc(Num * 1024 * 1024 * 1024 / 8);
get_mem_limit({mb, Num}) ->
    erlang:trunc(Num * 1024 * 1024 / 8);
get_mem_limit({kb, Num}) ->
    erlang:trunc(Num * 1024 / 8);
get_mem_limit({b, Num}) ->
    erlang:trunc(Num / 8).

set_ttl(Objects, ServerState, TTLOption) when erlang:is_list(Objects) ->
    [ok = set_ttl(Object, ServerState, TTLOption) || Object <- Objects],
    ok;

set_ttl(Object, ServerState, TTLOption) when erlang:is_tuple(Object) ->
    NewETSOptions = maps:get(new_ets_options, ServerState),
    MetaTable = maps:get(ets_meta_table, ServerState),
    TTLTable  = maps:get(ets_ttl_table , ServerState),
    FIFOTable = maps:get(ets_fifo_table, ServerState),
    LRUTable  = maps:get(ets_lru_table , ServerState),
    Key       = get_object_key(NewETSOptions, Object),
    Now       = get_now(),
    TTLTime   = get_ttl_time(TTLOption, Now),
    case ets:lookup(MetaTable, Key) of
        [] ->
            ignore;
        [{Key, OldTTLTime, OldInsertTime, OldUpdateTime}] ->
            true = ets:delete(TTLTable , {OldTTLTime, Key}),
            true = ets:delete(FIFOTable, {OldInsertTime, Key}),
            true = ets:delete(LRUTable , {OldUpdateTime, Key})
    end,
    %% meta table data structure :
    %% {key, ttltime, inserttime, updatetime}
    %% inserttime use for fifo
    %% updatetime use for lru
    true = ets:insert(MetaTable, {Key, TTLTime, Now, Now}),
    true = ets:insert(TTLTable , {{TTLTime, Key}, nouse}),
    true = ets:insert(FIFOTable, {{Now, Key}, nouse}),
    true = ets:insert(LRUTable , {{Now, Key}, nouse}),
    ok.

-spec update_lru_time(integer(), term(), map()) -> ok.
update_lru_time(Now, Key, ServerState) ->
    MetaTable = maps:get(ets_meta_table, ServerState),
    LRUTable  = maps:get(ets_lru_table , ServerState),
    [{Key, OldTTLTime, OldInsertTime, OldUpdateTime}] =
        ets:lookup(MetaTable, Key),
    true = ets:delete(LRUTable, {OldUpdateTime, Key}),
    true = ets:delete(MetaTable, Key),
    true = ets:insert(LRUTable, {{Now, Key}, nouse}),
    true = ets:insert(MetaTable, {Key, OldTTLTime, OldInsertTime, Now}),
    ok.

clean_unuse_via_ttl(ServerState) ->
    TTLTable = maps:get(ets_ttl_table, ServerState),
    true     = ets:safe_fixtable(TTLTable, true),
    clean_unuse_via_ttl(ets:first(TTLTable), TTLTable, ServerState),
    true     = ets:safe_fixtable(TTLTable, false),
    ok.

clean_unuse_via_ttl('$end_of_table', _, _) ->
    ok;
clean_unuse_via_ttl({TTLTime, OriginKey} = Key, TTLTable,
                    #{ets_main_table := MainTable} = ServerState) ->
    case get_now() > TTLTime of
        true ->
            ok   = clean_other_table_via_key(OriginKey, ServerState),
            true = ets:delete(MainTable, OriginKey),
            clean_unuse_via_ttl(ets:next(TTLTable, Key), TTLTable,
                                ServerState);
        false ->
            ok
    end.

clean_single_key_via_fifo(ServerState) ->
    FIFOTable = maps:get(ets_fifo_table, ServerState),
    MainTable = maps:get(ets_main_table, ServerState),
    case ets:first(FIFOTable) of
        '$end_of_table' ->
            ignore;
        {_, Key} ->
            ok = clean_other_table_via_key(Key, ServerState),
            true = ets:delete(MainTable, Key)
    end,
    ServerState.

clean_single_key_via_lru(ServerState) ->
    LRUTable  = maps:get(ets_lru_table , ServerState),
    MainTable = maps:get(ets_main_table, ServerState),
    case ets:first(LRUTable) of
        '$end_of_table' ->
            ignore;
        {_, Key} ->
            ok = clean_other_table_via_key(Key, ServerState),
            true = ets:delete(MainTable, Key)
    end,
    ServerState.

clean_other_table_via_key(OriginKey, ServerState) ->
    MetaTable = maps:get(ets_meta_table, ServerState),
    TTLTable  = maps:get(ets_ttl_table , ServerState),
    FIFOTable = maps:get(ets_fifo_table, ServerState),
    LRUTable  = maps:get(ets_lru_table , ServerState),
    [{OriginKey, TTLTime, InsertTime, UpdateTime}] =
        ets:lookup(MetaTable, OriginKey),
    true = ets:delete(MetaTable, OriginKey),
    true = ets:delete(TTLTable , {TTLTime, OriginKey}),
    true = ets:delete(FIFOTable, {InsertTime, OriginKey}),
    true = ets:delete(LRUTable , {UpdateTime, OriginKey}),
    ok.

get_object_key(NewETSOptions, Object) ->
    element(get_list_item(keypos, NewETSOptions, 1), Object).

get_ttl_time({sec, Num}, Now) ->
    Now + Num;
get_ttl_time({min, Num}, Now) ->
    Now + Num * 60;
get_ttl_time({hour, Num}, Now) ->
    Now + Num * 60 * 60.

get_now() ->
    {X1, X2, _} = os:timestamp(),
    X1 * 1000000 + X2.

% get_list_item(Key, PorpLORMap) ->
%     get_list_item(Key, PorpLORMap, undefined).

get_list_item(Key, PorpLORMap, Default) when erlang:is_list(PorpLORMap) ->
    case lists:keyfind(Key, 1, PorpLORMap) of
        {Key, Value} ->
            Value;
        _ ->
            Default
    end;
get_list_item(Key, PorpLORMap, Default) when erlang:is_map(PorpLORMap) ->
    maps:get(Key, PorpLORMap, Default);
get_list_item(_, _, Default) ->
    Default.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

gen_rets_test_() ->
    [ {"no new ets table", timeout, 20,
        fun() ->
            {ok, Pid} = gen_rets:start_link([]),
            timer:sleep(timer:seconds(11)),
            ?assertEqual(true, erlang:is_process_alive(Pid)),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"basic insert flow",
        fun() ->
            {ok, Pid} = gen_rets:start_link([]),
            gen_rets:new(Pid, test_for_ets, []),
            Table = gen_rets:get_ets(Pid),
            gen_rets:insert(Pid, [{akey, avalue}, {bkey, bvalue}], {sec, 2}),
            timer:sleep(timer:seconds(3)),
            ?assertEqual([], gen_rets:lookup(Pid, akey)),
            ?assertEqual([], gen_rets:lookup(Pid, bkey)),
            ?assertEqual(true, gen_rets:delete(Pid)),
            ?assertEqual(undefined, ets:info(Table))
        end}
    , {"repeated insert",
        fun() ->
            {ok, Pid} = gen_rets:start_link([]),
            gen_rets:new(Pid, test_for_ets, []),
            gen_rets:insert(Pid, {akey, avalue}, {sec, 1}),
            timer:sleep(timer:seconds(1)),
            gen_rets:insert(Pid, {akey, anewvalue}, {sec, 2}),
            ?assertEqual([{akey, anewvalue}], gen_rets:lookup(Pid, akey)),
            timer:sleep(1500),
            ?assertEqual([{akey, anewvalue}], gen_rets:lookup(Pid, akey)),
            timer:sleep(1500),
            ?assertEqual([], gen_rets:lookup(Pid, akey)),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"insert new", timeout, 10,
        fun() ->
            {ok, Pid} = gen_rets:start_link([]),
            gen_rets:new(Pid, test_for_ets, []),
            ?assertEqual(true , gen_rets:insert_new(Pid, {akey, avalue},
                                                    {sec, 4})),
            ?assertEqual(false, gen_rets:insert_new(Pid, {akey, aothervalue},
                                                    {sec, 6})),
            timer:sleep(timer:seconds(5)),
            ?assertEqual([], gen_rets:lookup(Pid, akey)),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"delete key",
        fun() ->
            {ok, Pid} = gen_rets:start_link([]),
            gen_rets:new(Pid, test_for_ets, []),
            gen_rets:insert(Pid, [{akey, avalue}, {bkey, bvalue}], {sec, 2}),
            ?assertEqual(true, gen_rets:delete(Pid, akey)),
            ?assertEqual([], gen_rets:lookup(Pid, akey)),
            ?assertEqual(true, gen_rets:delete(Pid, akey)),
            timer:sleep(3000),
            ?assertEqual([], gen_rets:lookup(Pid, bkey)),
            ?assertEqual(true, gen_rets:delete(Pid, bkey)),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"auto ttl 1", timeout, 20,
        fun() ->
            {ok, Pid} = gen_rets:start_link([]),
            gen_rets:new(Pid, test_for_ets, []),
            gen_rets:insert(Pid, {akey, avalue}, {sec, 3}),
            timer:sleep(timer:seconds(11)),
            ?assertEqual([], gen_rets:lookup(Pid, akey)),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"auto ttl 2", timeout, 20,
        fun() ->
            {ok, Pid} = gen_rets:start_link([]),
            gen_rets:new(Pid, test_for_ets, []),
            gen_rets:insert(Pid, {akey, avalue}, {sec, 3}),
            gen_rets:insert(Pid, {bkey, bvalue}, {sec, 15}),
            timer:sleep(timer:seconds(11)),
            ?assertEqual([], gen_rets:lookup(Pid, akey)),
            ?assertEqual([{bkey, bvalue}], gen_rets:lookup(Pid, bkey)),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"get ttl",
        fun() ->
            {ok, Pid} = gen_rets:start_link([]),
            gen_rets:new(Pid, test_for_ets, []),
            ?assertEqual(-1, gen_rets:get_ttl(Pid, fake_key)),
            ?assertEqual(true, gen_rets:insert(Pid, {akey, avalue}, {sec, 10})),
            ?assertEqual(true, 10 - gen_rets:get_ttl(Pid, akey) =< 1),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"reset ttl", timeout, 20,
        fun() ->
            {ok, Pid} = gen_rets:start_link([]),
            gen_rets:new(Pid, test_for_ets, []),
            Table = gen_rets:get_ets(Pid),
            ?assertEqual(-1, gen_rets:reset_ttl(Pid, fake_key, {sec, 10})),
            ?assertEqual(true, gen_rets:insert(Pid, {akey, avalue}, {sec, 5})),
            Now = get_now(),
            ServerState = sys:get_state(Pid),
            MetaTable = maps:get(ets_meta_table, ServerState),
            TTLTable  = maps:get(ets_ttl_table , ServerState),
            FIFOTable = maps:get(ets_fifo_table, ServerState),
            LRUTable  = maps:get(ets_lru_table , ServerState),
            [{akey, TTLTime, InsertTime, UpdateTime}] = ets:lookup(MetaTable, akey),
            ?assertEqual(true, Now + 5 - TTLTime =< 1),
            ?assertEqual(TTLTime, InsertTime + 5),
            ?assertEqual(TTLTime, UpdateTime + 5),
            ?assertEqual([{{TTLTime, akey}, nouse}], ets:tab2list(TTLTable)),
            ?assertEqual([{{InsertTime, akey}, nouse}], ets:tab2list(FIFOTable)),
            ?assertEqual([{{UpdateTime, akey}, nouse}], ets:tab2list(LRUTable)),
            ?assertEqual(true, gen_rets:reset_ttl(Pid, akey, {sec, 13})),
            [{akey, NewTTLTime, NewInsertTime, NewUpdateTime}] =
                ets:lookup(MetaTable, akey),
            ?assertEqual(true, get_now() + 13 - NewTTLTime =< 1),
            ?assertEqual(InsertTime, NewInsertTime),
            ?assertEqual([{{NewTTLTime, akey}, nouse}], ets:tab2list(TTLTable)),
            ?assertEqual([{{InsertTime, akey}, nouse}], ets:tab2list(FIFOTable)),
            ?assertEqual([{{NewUpdateTime, akey}, nouse}], ets:tab2list(LRUTable)),
            timer:sleep(timer:seconds(11)),
            ?assertEqual([{akey, avalue}], gen_rets:lookup(Pid, akey)),
            ?assertEqual(true, gen_rets:delete(Pid, akey)),
            ?assertEqual([], ets:tab2list(MetaTable)),
            ?assertEqual([], ets:tab2list(TTLTable)),
            ?assertEqual([], ets:tab2list(FIFOTable)),
            ?assertEqual([], ets:tab2list(LRUTable)),
            ?assertEqual([], ets:tab2list(Table)),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"fifo expire", timeout, 20,
        fun() ->
            {ok, Pid} = gen_rets:start_link([{expire_mode, fifo},
                                             {max_size, 3},
                                             {max_mem, {mb, 10}},
                                             {highwater_size, 2}]),
            gen_rets:new(Pid, test_for_ets, []),
            gen_rets:insert(Pid, {akey, avalue}, {sec, 20}),
            gen_rets:insert(Pid, {bkey, avalue}, {sec, 20}),
            gen_rets:insert(Pid, {ckey, avalue}, {sec, 20}),
            gen_rets:insert(Pid, {dkey, avalue}, {sec, 20}),
            ?assertEqual([], gen_rets:lookup(Pid, akey)),
            ?assertEqual([], gen_rets:lookup(Pid, bkey)),
            ?assertEqual([{ckey, avalue}], gen_rets:lookup(Pid, ckey)),
            ?assertEqual([{dkey, avalue}], gen_rets:lookup(Pid, dkey)),
            ServerState = sys:get_state(Pid),
            MetaTable = maps:get(ets_meta_table, ServerState),
            TTLTable  = maps:get(ets_ttl_table , ServerState),
            FIFOTable = maps:get(ets_fifo_table, ServerState),
            LRUTable  = maps:get(ets_lru_table , ServerState),
            ?assertEqual([], ets:lookup(MetaTable, akey)),
            ?assertEqual(2, ets:info(MetaTable, size)),
            ?assertEqual(2, ets:info(TTLTable , size)),
            ?assertEqual(2, ets:info(FIFOTable, size)),
            ?assertEqual(2, ets:info(LRUTable , size)),
            ?assertEqual([ckey, dkey],
                         lists:sort([X || {{_, X}, _} <- ets:tab2list(TTLTable)])),
            ?assertEqual([ckey, dkey],
                         lists:sort([X || {{_, X}, _} <- ets:tab2list(FIFOTable)])),
            ?assertEqual([ckey, dkey],
                         lists:sort([X || {{_, X}, _} <- ets:tab2list(LRUTable)])),
            ?assertEqual([ckey, dkey],
                         lists:sort([X || {X, _, _, _} <- ets:tab2list(MetaTable)])),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"lru expire", timeout, 20,
        fun() ->
            {ok, Pid} = gen_rets:start_link([{expire_mode, lru},
                                             {max_size, 5},
                                             {max_mem, {mb, 10}},
                                             {highwater_size, 3}]),
            gen_rets:new(Pid, test_for_ets, []),
            gen_rets:insert(Pid, {akey, avalue}, {sec, 20}),
            gen_rets:insert(Pid, {bkey, avalue}, {sec, 20}),
            gen_rets:insert(Pid, {ckey, avalue}, {sec, 20}),
            gen_rets:insert(Pid, {dkey, avalue}, {sec, 20}),
            gen_rets:insert(Pid, {ekey, avalue}, {sec, 20}),
            ServerState = sys:get_state(Pid),
            MetaTable = maps:get(ets_meta_table, ServerState),
            TTLTable  = maps:get(ets_ttl_table , ServerState),
            FIFOTable = maps:get(ets_fifo_table, ServerState),
            LRUTable  = maps:get(ets_lru_table , ServerState),
            timer:sleep(timer:seconds(1)),
            ?assertEqual([{akey, avalue}], gen_rets:lookup(Pid, akey)),
            ?assertEqual([{bkey, avalue}], gen_rets:lookup(Pid, bkey)),
            ?assertEqual([{ckey, avalue}], gen_rets:lookup(Pid, ckey)),
            gen_rets:insert(Pid, {fkey, avalue}, {sec, 20}),
            ?assertEqual([], gen_rets:lookup(Pid, akey)),
            ?assertEqual([{bkey, avalue}], gen_rets:lookup(Pid, bkey)),
            ?assertEqual([{ckey, avalue}], gen_rets:lookup(Pid, ckey)),
            ?assertEqual([], gen_rets:lookup(Pid, dkey)),
            ?assertEqual([], gen_rets:lookup(Pid, ekey)),
            ?assertEqual([{fkey, avalue}], gen_rets:lookup(Pid, fkey)),
            ?assertEqual([], ets:lookup(MetaTable, akey)),
            ?assertEqual(3, ets:info(MetaTable, size)),
            ?assertEqual(3, ets:info(TTLTable , size)),
            ?assertEqual(3, ets:info(FIFOTable, size)),
            ?assertEqual(3, ets:info(LRUTable , size)),
            ?assertEqual([bkey, ckey, fkey],
                         lists:sort([X || {{_, X}, _} <- ets:tab2list(TTLTable)])),
            ?assertEqual([bkey, ckey, fkey],
                         lists:sort([X || {{_, X}, _} <- ets:tab2list(FIFOTable)])),
            ?assertEqual([bkey, ckey, fkey],
                         lists:sort([X || {{_, X}, _} <- ets:tab2list(LRUTable)])),
            ?assertEqual([bkey, ckey, fkey],
                         lists:sort([X || {X, _, _, _} <- ets:tab2list(MetaTable)])),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"fifo expire via mem",
        fun() ->
            {ok, Pid} = gen_rets:start_link([{expire_mode, fifo},
                                             {max_mem, {b, 32000}},
                                             {highwater_mem, {b, 19300}}]),
            gen_rets:new(Pid, test_for_ets, []),
            Value = lists:seq(1, 200),
            ?assertEqual(true, gen_rets:insert(Pid, [{X, Value}
                                                     || X <- lists:seq(1, 12)],
                                               {sec, 20})),
            [?assertEqual([{X, Value}], gen_rets:lookup(Pid, X))
             || X <- lists:seq(10, 12)],
            [?assertEqual([], gen_rets:lookup(Pid, X))
             ||X <- lists:seq(1, 3)],
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"lru expire via mem",
        fun() ->
            {ok, Pid} = gen_rets:start_link([{expire_mode, lru},
                                             {max_mem, {b, 32000}},
                                             {highwater_mem, {b, 19300}}]),
            gen_rets:new(Pid, test_for_ets, []),
            Value = lists:seq(1, 200),
            ?assertEqual(true, gen_rets:insert(Pid, [{X, Value}
                                                     || X <- lists:seq(1, 12)],
                                               {sec, 20})),
            [?assertEqual([{X, Value}], gen_rets:lookup(Pid, X))
             || X <- lists:seq(10, 12)],
            [?assertEqual([], gen_rets:lookup(Pid, X))
             ||X <- lists:seq(1, 3)],
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"delete_object",
        fun() ->
            {ok, Pid} = gen_rets:start_link([]),
            gen_rets:new(Pid, test_for_ets, [bag]),
            ?assertEqual(true, gen_rets:insert(Pid, [{a, v1}, {a, v2}], {sec, 10})),
            ?assertEqual(true, gen_rets:delete_object(Pid, {c, v1})),
            ?assertEqual(true, gen_rets:delete_object(Pid, {a, v1})),
            ?assertEqual([{a, v2}], gen_rets:lookup(Pid, a)),
            ServerState = sys:get_state(Pid),
            MainTable = maps:get(ets_main_table, ServerState),
            MetaTable = maps:get(ets_meta_table, ServerState),
            TTLTable  = maps:get(ets_ttl_table , ServerState),
            FIFOTable = maps:get(ets_fifo_table, ServerState),
            LRUTable  = maps:get(ets_lru_table , ServerState),
            ?assertEqual(a, ets:first(MetaTable)),
            ?assertEqual(a, erlang:element(2, ets:first(TTLTable))),
            ?assertEqual(a, erlang:element(2, ets:first(LRUTable))),
            ?assertEqual(a, erlang:element(2, ets:first(FIFOTable))),
            ?assertEqual(true, gen_rets:delete_object(Pid, {a, v2})),
            ?assertEqual([], ets:tab2list(MainTable)),
            ?assertEqual([], ets:tab2list(MetaTable)),
            ?assertEqual([], ets:tab2list(TTLTable)),
            ?assertEqual([], ets:tab2list(FIFOTable)),
            ?assertEqual([], ets:tab2list(LRUTable)),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"delete_all_objects",
        fun() ->
            {ok, Pid} = gen_rets:start_link([]),
            gen_rets:new(Pid, test_for_ets, [bag]),
            ?assertEqual(true, gen_rets:insert(Pid, [{a, v1}, {a, v2}], {sec, 10})),
            ?assertEqual(true, gen_rets:delete_all_objects(Pid)),
            ServerState = sys:get_state(Pid),
            MainTable = maps:get(ets_main_table, ServerState),
            MetaTable = maps:get(ets_meta_table, ServerState),
            TTLTable  = maps:get(ets_ttl_table , ServerState),
            FIFOTable = maps:get(ets_fifo_table, ServerState),
            LRUTable  = maps:get(ets_lru_table , ServerState),
            ?assertEqual([], ets:tab2list(MainTable)),
            ?assertEqual([], ets:tab2list(MetaTable)),
            ?assertEqual([], ets:tab2list(TTLTable)),
            ?assertEqual([], ets:tab2list(FIFOTable)),
            ?assertEqual([], ets:tab2list(LRUTable)),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    ].

-endif.