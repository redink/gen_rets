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
        , start/1
        , get_ets/1
        , insert/2
        , insert/3
        , insert_new/3
        , delete/1
        , delete/2
        , delete_object/2
        , delete_all_objects/1
        , lookup/2
        , update_counter/3
        , update_counter/5
        , update_element/3
        , get_ttl/2
        , reset_ttl/3
        , subscribe/1
        ]).

%% use for recover from aof.
-export([ call_insert/4
        , call_insert_new/4
        , call_delete/3
        , call_delete_object/3
        , call_update_counter/5
        , call_update_counter/7
        , call_update_element/5
        , call_reset_ttl/4
        ]).

-export([ get_list_item/2
        , get_list_item/3
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-define(SERVER, ?MODULE).
-define(HIBERNATE_TIMEOUT, 1000).
-define(MAXTTLTIME, {hour, 24 * 365 * 100}).

-ifdef(TEST).
-define(DELETEKEYTABLELIMIT, 2).
-else.
-define(DELETEKEYTABLELIMIT, 10).
-endif.

% -callback

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------

-spec get_ets(pid() | atom()) -> ets:tid() | atom().
get_ets(ServerPid) ->
    gen_server:call(ServerPid, get_ets).

-spec insert(pid() | atom(), tuple() | [tuple()]) -> boolean().
insert(ServerPid, Objects) ->
    insert(ServerPid, Objects, ?MAXTTLTIME).

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

update_counter(ServerPid, Key, UpdateOp) ->
    gen_server:call(ServerPid, {update_counter, Key, UpdateOp}).
update_counter(ServerPid, Key, UpdateOp, Default, TTLOption) ->
    gen_server:call(ServerPid, {update_counter, Key, UpdateOp,
                                Default, TTLOption}).

-spec update_element(pid() | atom(), term(),
                     {integer(), term()} |
                     [{integer(), term()}]) -> boolean().
update_element(ServerPid, Key, ElementSpec) ->
    gen_server:call(ServerPid, {update_element, Key, ElementSpec}).

-spec get_ttl(pid() | atom(), term()) -> integer().
get_ttl(ServerPid, Key) ->
    gen_server:call(ServerPid, {get_ttl, Key}).

-spec reset_ttl(pid() | atom(), term(),
                {sec, integer()} |
                {min, integer()} |
                {hour, integer()}) -> -1 | true.
reset_ttl(ServerPid, Key, TTLOption) ->
    gen_server:call(ServerPid, {reset_ttl, Key, TTLOption}).

-spec subscribe(pid() | atom()) -> ok.
subscribe(ServerPid) ->
    gen_server:call(ServerPid, {subscribe, erlang:self()}).

start_link(Options) ->
    case get_list_item(name, Options, -1) of
        Name when erlang:is_atom(Name) ->
            gen_server:start_link({local, Name}, ?MODULE, [Options], []);
        _ ->
            gen_server:start_link(?MODULE, [Options], [])
    end.

start(Options) ->
    case get_list_item(name, Options, -1) of
        Name when erlang:is_atom(Name) ->
            gen_server:start({local, Name}, ?MODULE, [Options], []);
        _ ->
            gen_server:start(?MODULE, [Options], [])
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Options]) ->
    ExpireMode    = get_list_item(expire_mode   , Options, fifo),
    MaxSize       = get_list_item(max_size      , Options, 100000),
    HighWaterSize = get_list_item(highwater_size, Options, 70000),
    MaxMem        = get_mem_limit(get_list_item(max_mem, Options, {gb, 2})),
    HighWaterMem  = get_mem_limit(get_list_item(highwater_mem, Options, {gb, 1.5})),
    {sec, TimeInterval} = get_list_item(check_ttl_interval, Options, {sec, 10}),
    %% new ets table
    EtsTableName  = get_list_item(ets_table_name , Options),
    NewETSOptions = get_list_item(new_ets_options, Options, []),
    case {lists:member(named_table, NewETSOptions),
          get_list_item(persistence, Options, false),
          lists:member(public, NewETSOptions)} of
        {true, false, _} ->
            MetaTable = ets:new(ets_meta_table, [public, set, compressed]),
            TTLTable  = ets:new(ets_ttl_table , [public, ordered_set, compressed]),
            FIFOTable = ets:new(ets_fifo_table, [public, ordered_set, compressed]),
            LRUTable  = ets:new(ets_lru_table , [public, ordered_set, compressed]),
            MainTable = ets:new(EtsTableName, NewETSOptions),
            erlang:send_after(timer:seconds(TimeInterval), erlang:self(), ttl_clean),
            {ok, #{ expire_mode => ExpireMode
                  , max_mem     => MaxMem
                  , max_size    => MaxSize
                  , highwater_mem  => HighWaterMem
                  , highwater_size => HighWaterSize
                  , deleted_key_table  => ets:new(deleted_key_table, [])
                  , check_ttl_interval => TimeInterval
                  , ets_main_table  => MainTable
                  , ets_meta_table  => MetaTable
                  , ets_ttl_table   => TTLTable
                  , ets_fifo_table  => FIFOTable
                  , ets_lru_table   => LRUTable
                  , new_ets_options => NewETSOptions
                  }, ?HIBERNATE_TIMEOUT};
        {true, aof, true} ->
            MetaTable = ets:new(ets_meta_table, [public, set, compressed]),
            TTLTable  = ets:new(ets_ttl_table , [public, ordered_set, compressed]),
            FIFOTable = ets:new(ets_fifo_table, [public, ordered_set, compressed]),
            LRUTable  = ets:new(ets_lru_table , [public, ordered_set, compressed]),
            MainTable = ets:new(EtsTableName, NewETSOptions),
            State     = #{ expire_mode => ExpireMode
                         , max_mem     => MaxMem
                         , max_size    => MaxSize
                         , highwater_mem  => HighWaterMem
                         , highwater_size => HighWaterSize
                         , deleted_key_table  => ets:new(deleted_key_table, [])
                         , check_ttl_interval => TimeInterval
                         , ets_main_table  => MainTable
                         , ets_meta_table  => MetaTable
                         , ets_ttl_table   => TTLTable
                         , ets_fifo_table  => FIFOTable
                         , ets_lru_table   => LRUTable
                         , new_ets_options => NewETSOptions
                         },
            {ok, AOFPid} = rets_aof:start_link([{ets_table_name, EtsTableName},
                                                {server_state, State}]),
            erlang:send_after(timer:seconds(TimeInterval), erlang:self(), ttl_clean),
            erlang:send(erlang:self(), refresh_main_table),
            {ok, State#{ aof_pid => AOFPid}, ?HIBERNATE_TIMEOUT};
        {false, false, _} ->
            MetaTable = ets:new(ets_meta_table, [public, set, compressed]),
            TTLTable  = ets:new(ets_ttl_table , [public, ordered_set, compressed]),
            FIFOTable = ets:new(ets_fifo_table, [public, ordered_set, compressed]),
            LRUTable  = ets:new(ets_lru_table , [public, ordered_set, compressed]),
            MainTable = ets:new(EtsTableName, NewETSOptions),
            erlang:send_after(timer:seconds(TimeInterval), erlang:self(), ttl_clean),
            {ok, #{ expire_mode => ExpireMode
                  , max_mem     => MaxMem
                  , max_size    => MaxSize
                  , highwater_mem  => HighWaterMem
                  , highwater_size => HighWaterSize
                  , deleted_key_table  => ets:new(deleted_key_table, [])
                  , check_ttl_interval => TimeInterval
                  , ets_main_table     => MainTable
                  , ets_meta_table     => MetaTable
                  , ets_ttl_table      => TTLTable
                  , ets_fifo_table     => FIFOTable
                  , ets_lru_table      => LRUTable
                  , new_ets_options    => NewETSOptions
                  }, ?HIBERNATE_TIMEOUT};
        _ ->
            {stop, "aof should named table or public"}
    end.

%%--------------------------------------------------------------------

handle_call(get_ets, _From, #{ets_main_table := MainTable} = State) ->
    {reply, MainTable, State, ?HIBERNATE_TIMEOUT};

handle_call({insert, Objects, TTLOption}, _From, State) ->
    Now  = get_now(),
    true = call_insert(State, Now, Objects, TTLOption),
    ok   = trigger_aof(State, {?MODULE, call_insert},
                       [Now, Objects, TTLOption]),
    refresh_main_table(normal),
    {reply, true, State, ?HIBERNATE_TIMEOUT};

handle_call({insert_new, Objects, TTLOption}, _From, State) ->
    Now = get_now(),
    case R = call_insert_new(State, Now, Objects, TTLOption) of
        true ->
            refresh_main_table(normal);
        false ->
            ignore
    end,
    ok = trigger_aof(State, {?MODULE, call_insert_new},
                     [Now, Objects, TTLOption]),
    {reply, R, State, ?HIBERNATE_TIMEOUT};

handle_call(delete, _From, State) ->
    ok = trigger_aof(State, delete_aof_log, []),
    {stop, normal, true, State};

handle_call({delete, Key}, _From, State) ->
    true = call_delete(State, log_deleted_key, Key),
    ok   = trigger_aof(State, {?MODULE, call_delete},
                       [no, Key]),
    {reply, true, State, ?HIBERNATE_TIMEOUT};

handle_call({delete_object, Object}, _From, State) ->
    true = call_delete_object(State, log_deleted_key, Object),
    ok   = trigger_aof(State, {?MODULE, call_delete_object},
                       [no, Object]),
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
    ok   = trigger_aof(State, clean_aof_log, []),
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
                        call_delete(State, log_deleted_key, Key),
                        %% no need to trigger aof
                        %% the key is expired, when recover from aof
                        %% insert/insert_new op will ignore it
                        [];
                    _ ->
                        update_lru_time(Now, Key, State),
                        %% TODO trigger aof for update lru
                        ets:lookup(MainTable, Key)
                end,
            {reply, Return, State, ?HIBERNATE_TIMEOUT}
    end;

handle_call({update_counter, Key, UpdateOp}, _From, State) ->
    Now = get_now(),
    R   = call_update_counter(State, log_deleted_key, Now, Key, UpdateOp),
    ok  = trigger_aof(State, {?MODULE, call_update_counter},
                      [no, Now, Key, UpdateOp]),
    {reply, R, State, ?HIBERNATE_TIMEOUT};

handle_call({update_counter, Key, UpdateOp, Default, TTLOption}, _From, State) ->
    Now = get_now(),
    R   = call_update_counter(State, log_deleted_key, Now, Key, UpdateOp,
                              Default, TTLOption),
    ok  = trigger_aof(State, {?MODULE, call_update_counter},
                      [no, Now, Key, UpdateOp, Default, TTLOption]),
    refresh_main_table(normal),
    {reply, R, State, ?HIBERNATE_TIMEOUT};

handle_call({update_element, Key, ElementSpec}, _From, State) ->
    Now = get_now(),
    R   = call_update_element(State, log_deleted_key, Now, Key, ElementSpec),
    ok  = trigger_aof(State, {?MODULE, call_update_element},
                      [no, Now, Key, ElementSpec]),
    refresh_main_table(normal),
    {reply, R, State, ?HIBERNATE_TIMEOUT};

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

handle_call({reset_ttl, Key, Time}, _From, State) ->
    Now = get_now(),
    R   = call_reset_ttl(State, Now, Key, Time),
    ok  = trigger_aof(State, {?MODULE, reset_ttl},
                      [Now, Key, Time]),
    {reply, R, State, ?HIBERNATE_TIMEOUT};

handle_call({subscribe, Subscriber}, _From, State) ->
    OldSubscribeList = maps:get(subscribe_list, State, []),
    {reply, ok, State#{subscribe_list => [Subscriber | OldSubscribeList]},
     ?HIBERNATE_TIMEOUT};

handle_call(exit, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, unsupported, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------

handle_info(ttl_clean, #{check_ttl_interval := TimeInterval} = State) ->
    case maps:get(ets_main_table, State, -1) of
        -1 ->
            ignore;
        _ ->
            ok = clean_unuse_via_ttl(State),
            erlang:send_after(timer:seconds(TimeInterval),
                              erlang:self(), ttl_clean)
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
            #{ expire_mode := ExpireMode
             , ets_main_table := MainTable} = State) ->
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

handle_info({log_deleted_key, DeleteKey}, State) ->
    DeleteKeyTable = maps:get(deleted_key_table, State),
    ets:insert(DeleteKeyTable, {DeleteKey, nouse}),
    NewTimeoutRef =
        case {ets:info(DeleteKeyTable, size) >= ?DELETEKEYTABLELIMIT,
              maps:get(timeout_ref, State, -1)} of
            {false, -1} ->
                erlang:send_after(timer:seconds(1), erlang:self(), notify_subscriber);
            {false, OldTimeoutRef} ->
                OldTimeoutRef;
            {true, OldTimeoutRef} ->
                ok = notify_subscriber(State),
                _  = erlang:cancel_timer(OldTimeoutRef),
                erlang:send_after(timer:seconds(1), erlang:self(), notify_subscriber)
        end,
    {noreply, State#{timeout_ref => NewTimeoutRef}, ?HIBERNATE_TIMEOUT};

handle_info(notify_subscriber, State) ->
    ok = notify_subscriber(State),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(timeout, State) ->
    refresh_main_table(force),
    proc_lib:hibernate(gen_server, enter_loop,
               [?MODULE, [], State]),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(_Info, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
terminate(normal, State) ->
    ok = trigger_aof(State, close_aof_log, []),
    ok;
terminate(_Reason, State) ->
    ok = trigger_aof(State, delete_aof_log, []),
    ok.
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec call_insert(map(), integer(), tuple() | [tuple()],
                  {sec | min | hour, integer()}) -> true.
call_insert(#{ets_main_table := MainTable} = State,
            StandardNow, Objects, TTLOption) ->
    ExpireTime = get_expire_time(TTLOption, StandardNow),
    case get_now() < ExpireTime of
        true ->
            true = ets:insert(MainTable, Objects),
            ok   = set_ttl(StandardNow, Objects, State, TTLOption);
        _ ->
            ignore
    end,
    true.

-spec call_insert_new(map(), integer(), tuple() | [tuple()],
                      {sec | min | hour, integer()}) -> boolean().
call_insert_new(#{ets_main_table := MainTable} = State,
                StandardNow, Objects, TTLOption) ->
    ExpireTime = get_expire_time(TTLOption, StandardNow),
    case get_now() < ExpireTime of
        true ->
            case ets:insert_new(MainTable, Objects) of
                true ->
                    ok = set_ttl(StandardNow, Objects, State, TTLOption),
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end.

-spec call_delete(map(), atom(), term()) -> true.
call_delete(#{ets_main_table := MainTable} = State, IsLog, Key) ->
    case ets:lookup(MainTable, Key) of
        [] ->
            ignore;
        _ ->
            ok = clean_other_table_via_key(IsLog, Key, State),
            true = ets:delete(MainTable, Key)
    end,
    true.

call_delete_object(#{ ets_main_table := MainTable
                    , new_ets_options := NewETSOptions} = State,
                   IsLog, Object) ->
    Key = get_object_key(NewETSOptions, Object),
    case ets:lookup(MainTable, Key) of
        [] ->
            ignore;
        [_] ->
            ok = clean_other_table_via_key(IsLog, Key, State),
            ets:delete_object(MainTable, Object);
        [_ | _] ->
            ets:delete_object(MainTable, Object)
    end,
    true.

call_update_counter(#{ ets_meta_table := MetaTable
                     , ets_main_table := MainTable} = State,
                    IsLog, StandardNow, Key, UpdateOp) ->
    case ets:lookup(MetaTable, Key) of
        [] ->
            not_found;
        [{Key, TTLTime, _, _}] ->
            case StandardNow > TTLTime of
                true ->
                    call_delete(State, IsLog, Key),
                    not_found;
                _ ->
                    update_lru_time(StandardNow, Key, State),
                    ets:update_counter(MainTable, Key, UpdateOp)
            end
    end.

-spec call_update_counter(map(), atom(), integer(), term(), term(), tuple(),
                          {sec | min | hour, integer()}) -> term().
call_update_counter(#{ ets_meta_table := MetaTable
                     , ets_main_table := MainTable} = State,
                    IsLog, StandardNow, Key, UpdateOp, Default, TTLOption) ->
    case get_now() < get_expire_time(TTLOption, StandardNow) of
        true ->
            case ets:lookup(MetaTable, Key) of
                [] ->
                    call_insert(State, StandardNow, Default, TTLOption),
                    ets:update_counter(MainTable, Key, UpdateOp);
                [{Key, TTLTime, _, _}] ->
                    case StandardNow > TTLTime of
                        true ->
                            call_delete(State, IsLog, Key),
                            not_found;
                        _ ->
                            update_lru_time(StandardNow, Key, State),
                            ets:update_counter(MainTable, Key, UpdateOp)
                    end
            end;
        _ ->
            not_found
    end.

-spec call_update_element(map(), atom(), integer(), term(),
                          {integer(), term()} |
                          [{integer(), term()}]) -> boolean().
call_update_element(#{ ets_meta_table := MetaTable
                     , ets_main_table := MainTable} = State,
                    IsLog, StandardNow, Key, ElementSpec) ->
    case ets:lookup(MetaTable, Key) of
        [] ->
            false;
        [{Key, TTLTime, _, _}] ->
            case StandardNow > TTLTime of
                true ->
                    call_delete(State, IsLog, Key),
                    false;
                _ ->
                    update_lru_time(StandardNow, Key, State),
                    ets:update_element(MainTable, Key, ElementSpec)
            end
    end.

-spec call_reset_ttl(map(), integer(), term(),
                     {sec | min | hour, integer()}) -> -1 | true.
call_reset_ttl(#{ ets_meta_table := MetaTable
                , ets_lru_table  := LRUTable
                , ets_ttl_table  := TTLTable},
               StandardNow, Key, TTLOption) ->
    case get_now() < get_expire_time(TTLOption, StandardNow) of
        true ->
            case ets:lookup(MetaTable, Key) of
                [] ->
                    -1;
                [{Key, OldTTLTime, OldInsertTime, OldUpdateTime}] ->
                    TTLTime = get_expire_time(TTLOption, StandardNow),
                    true = ets:delete(TTLTable, {OldTTLTime, Key}),
                    true = ets:delete(LRUTable, {OldUpdateTime, Key}),
                    true = ets:insert(TTLTable, {{TTLTime, Key}, nouse}),
                    true = ets:insert(LRUTable, {{StandardNow, Key}, nouse}),
                    true = ets:insert(MetaTable,
                                      {Key, TTLTime, OldInsertTime, StandardNow}),
                    true
            end;
        _ ->
            -1
    end.

notify_subscriber(ServerState) ->
    DeleteKeyTable = maps:get(deleted_key_table, ServerState),
    case maps:get(subscribe_list, ServerState, []) of
        [] ->
            ignore;
        SubscribeList ->
            DeleteKeyList = [X || {X, _} <- ets:tab2list(DeleteKeyTable)],
            [erlang:send(X, {auto_deleted, DeleteKeyList})
             || X <- SubscribeList]
    end,
    ets:delete_all_objects(DeleteKeyTable),
    ok.

get_mem_limit({gb, Num}) ->
    erlang:trunc(Num * 1024 * 1024 * 1024 / 8);
get_mem_limit({mb, Num}) ->
    erlang:trunc(Num * 1024 * 1024 / 8);
get_mem_limit({kb, Num}) ->
    erlang:trunc(Num * 1024 / 8);
get_mem_limit({b, Num}) ->
    erlang:trunc(Num / 8).

trigger_aof(ServerState, FunName, Args) ->
    case maps:get(aof_pid, ServerState, -1) of
        AOFPid when erlang:is_pid(AOFPid) ->
            MainTable = maps:get(ets_main_table, ServerState),
            gen_server:call(AOFPid, {aof, MainTable, FunName, Args}),
            ok;
        _ ->
            ok
    end.

set_ttl(Now, Objects, ServerState, TTLOption) when erlang:is_list(Objects) ->
    [ok = set_ttl(Now, Object, ServerState, TTLOption) || Object <- Objects],
    ok;

set_ttl(Now, Object, ServerState, TTLOption) when erlang:is_tuple(Object) ->
    NewETSOptions = maps:get(new_ets_options, ServerState),
    MetaTable = maps:get(ets_meta_table, ServerState),
    TTLTable  = maps:get(ets_ttl_table , ServerState),
    FIFOTable = maps:get(ets_fifo_table, ServerState),
    LRUTable  = maps:get(ets_lru_table , ServerState),
    Key       = get_object_key(NewETSOptions, Object),
    TTLTime   = get_expire_time(TTLOption, Now),
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
    clean_other_table_via_key(log_deleted_key, OriginKey, ServerState).

clean_other_table_via_key(IsLog, OriginKey, ServerState) ->
    MetaTable = maps:get(ets_meta_table, ServerState),
    TTLTable  = maps:get(ets_ttl_table , ServerState),
    FIFOTable = maps:get(ets_fifo_table, ServerState),
    LRUTable  = maps:get(ets_lru_table , ServerState),
    case ets:lookup(MetaTable, OriginKey) of
        [] ->
            ignore;
        [{OriginKey, TTLTime, InsertTime, UpdateTime}] ->
            true = ets:delete(MetaTable, OriginKey),
            true = ets:delete(TTLTable , {TTLTime, OriginKey}),
            true = ets:delete(FIFOTable, {InsertTime, OriginKey}),
            true = ets:delete(LRUTable , {UpdateTime, OriginKey})
    end,
    case IsLog of
        log_deleted_key ->
            erlang:send(erlang:self(), {log_deleted_key, OriginKey});
        _ ->
            ignore
    end,
    ok.

get_object_key(NewETSOptions, Object) ->
    element(get_list_item(keypos, NewETSOptions, 1), Object).

get_expire_time({sec, Num}, Now) ->
    Now + Num;
get_expire_time({min, Num}, Now) ->
    Now + Num * 60;
get_expire_time({hour, Num}, Now) ->
    Now + Num * 60 * 60.

get_now() ->
    {X1, X2, _} = os:timestamp(),
    X1 * 1000000 + X2.

get_list_item(Key, PorpLORMap) ->
    get_list_item(Key, PorpLORMap, undefined).

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
refresh_main_table(_) ->
    erlang:send(erlang:self(), refresh_main_table).
-else.
refresh_main_table(normal) ->
    ignore;
refresh_main_table(force) ->
    erlang:send(erlang:self(), refresh_main_table).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

gen_rets_test_() ->
    [ {"basic insert flow",
        fun() ->
            {ok, Pid} = gen_rets:start_link([{ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
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
            {ok, Pid} = gen_rets:start_link([{ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
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
            {ok, Pid} = gen_rets:start_link([{ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
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
            {ok, Pid} = gen_rets:start_link([{ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
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
            {ok, Pid} = gen_rets:start_link([{ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
            ?assertEqual(ok, gen_rets:subscribe(Pid)),
            gen_rets:insert(Pid, {akey, avalue}, {sec, 3}),
            timer:sleep(timer:seconds(11)),
            ?assertEqual([], gen_rets:lookup(Pid, akey)),
            receive
                {auto_deleted, KL} ->
                    ?assertEqual([akey], KL)
            end,
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"auto ttl 2", timeout, 20,
        fun() ->
            {ok, Pid} = gen_rets:start_link([{ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
            ?assertEqual(ok, gen_rets:subscribe(Pid)),
            gen_rets:insert(Pid, {akey, avalue}, {sec, 3}),
            gen_rets:insert(Pid, {bkey, bvalue}, {sec, 15}),
            timer:sleep(timer:seconds(11)),
            ?assertEqual([], gen_rets:lookup(Pid, akey)),
            ?assertEqual([{bkey, bvalue}], gen_rets:lookup(Pid, bkey)),
            receive
                {auto_deleted, KL} ->
                    ?assertEqual([akey], KL)
            end,
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"auto ttl 3", timeout, 20,
        fun() ->
            {ok, Pid} = gen_rets:start_link([{ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
            ?assertEqual(ok, gen_rets:subscribe(Pid)),
            gen_rets:insert(Pid, {akey, avalue}, {sec, 3}),
            gen_rets:insert(Pid, {bkey, bvalue}, {sec, 3}),
            timer:sleep(timer:seconds(11)),
            ?assertEqual([], gen_rets:lookup(Pid, akey)),
            ?assertEqual([], gen_rets:lookup(Pid, bkey)),
            receive
                {auto_deleted, KL} ->
                    ?assertEqual([akey, bkey], lists:sort(KL))
            end,
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"get ttl",
        fun() ->
            {ok, Pid} = gen_rets:start_link([{ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
            ?assertEqual(-1, gen_rets:get_ttl(Pid, fake_key)),
            ?assertEqual(true, gen_rets:insert(Pid, {akey, avalue}, {sec, 10})),
            ?assertEqual(true, 10 - gen_rets:get_ttl(Pid, akey) =< 1),
            ?assertEqual(true, gen_rets:delete(Pid))
        end}
    , {"reset ttl", timeout, 20,
        fun() ->
            {ok, Pid} = gen_rets:start_link([{ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
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
                                             {highwater_size, 2},
                                             {ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
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
                                             {highwater_size, 3},
                                             {ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
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
                                             {highwater_mem, {b, 19300}},
                                             {ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
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
                                             {highwater_mem, {b, 19300}},
                                             {ets_table_name, test_for_ets},
                                             {new_ets_options, []}]),
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
            {ok, Pid} = gen_rets:start_link([{ets_table_name, 'delete_object'},
                                             {new_ets_options, [named_table, public, bag]},
                                             {name, 'delete_object'},
                                             {persistence, aof}]),
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
            ?assertEqual(ok, gen_server:call(Pid, exit)),
            {ok, NewPid} = gen_rets:start_link([{ets_table_name, 'delete_object'},
                                                {new_ets_options, [named_table, public, bag]},
                                                {name, 'delete_object'},
                                                {persistence, aof}]),
            ?assertEqual(0, ets:info('delete_object', size)),
            ?assertEqual(true, gen_rets:delete(NewPid))
        end}
    , {"delete_all_objects",
        fun() ->
            {ok, Pid} = gen_rets:start_link([{ets_table_name, 'delete_all_objects'},
                                             {new_ets_options, [named_table, public, bag]},
                                             {name, 'delete_all_objects'},
                                             {persistence, aof}]),
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
    , {"update_counter/3",
        fun() ->
            {ok, Pid} = gen_rets:start_link([{ets_table_name, 'update_counter_test_3'},
                                             {new_ets_options, [named_table, public]},
                                             {name, 'update_counter_test/3'},
                                             {persistence, aof}]),
            'update_counter_test/3'(Pid)
        end}
    , {"update_counter/4", timeout, 20,
        fun() ->
            {ok, Pid} = gen_rets:start_link([{ets_table_name, '18_update_counter_test_4'},
                                             {new_ets_options, [named_table, public]},
                                             {name, '18_update_counter_test_4'},
                                             {persistence, aof}]),
            '18_update_counter_test/4'(Pid)
        end}
    , {"update_element/3", timeout, 20,
        fun() ->
            {ok, Pid} = gen_rets:start_link([{ets_table_name, 'update_element_3'},
                                             {new_ets_options, [named_table, public]},
                                             {name, 'update_element_3'},
                                             {persistence, aof}]),
            ?assertEqual(false, gen_rets:update_element(Pid, "redink", {2, "sichuan"})),
            ?assertEqual(true, gen_rets:insert(Pid, {"redink", "sichuan"}, {sec, 5})),
            ?assertEqual(true, gen_rets:update_element(Pid, "redink", {2, "beijing"})),
            ?assertEqual([{"redink", "beijing"}], gen_rets:lookup(Pid, "redink")),
            ?assertEqual(ok, gen_server:call(Pid, exit)),
            {ok, Pid1} = gen_rets:start_link([{ets_table_name, 'update_element_3'},
                                              {new_ets_options, [named_table, public]},
                                              {name, 'update_element_3'},
                                              {persistence, aof}]),
            ?assertEqual([{"redink", "beijing"}], gen_rets:lookup(Pid1, "redink")),
            ?assertEqual(ok, gen_server:call(Pid1, exit)),
            timer:sleep(timer:seconds(5)),
            {ok, Pid2} = gen_rets:start_link([{ets_table_name, 'update_element_3'},
                                              {new_ets_options, [named_table, public]},
                                              {name, 'update_element_3'},
                                              {persistence, aof}]),
            ?assertEqual([], gen_rets:lookup(Pid2, "redink")),
            ?assertEqual(true, gen_rets:delete(Pid2))
        end}
    ].

'update_counter_test/3'(Pid) ->
    ?assertEqual(true, gen_rets:insert(Pid, {"redink", 26, "coder"}, {sec, 10})),
    gen_rets:update_counter(Pid, "redink", {2, 1}),
    ?assertEqual([{"redink", 27, "coder"}], gen_rets:lookup(Pid, "redink")),
    ?assertEqual(true, gen_rets:delete(Pid, "redink")),

    ?assertEqual(true, gen_rets:insert(Pid, {"redink", 26}, {sec, 10})),
    gen_rets:update_counter(Pid, "redink", 1),
    ?assertEqual([{"redink", 27}], gen_rets:lookup(Pid, "redink")),
    ?assertEqual(true, gen_rets:delete(Pid, "redink")),

    ?assertEqual(true, gen_rets:insert(Pid, {"redink", 26, 1}, {sec, 20})),
    gen_rets:update_counter(Pid, "redink", [{2, 1}, {3, 11, 10, 3}]),
    ?assertEqual([{"redink", 27, 3}], gen_rets:lookup(Pid, "redink")),
    ok = gen_server:call(Pid, exit),
    {ok, NewPid} = gen_rets:start_link([{ets_table_name, 'update_counter_test_3'},
                                        {new_ets_options, [named_table, public]},
                                        {name, 'update_counter_test_3'},
                                        {persistence, aof}]),
    ?assertEqual([{"redink", 27, 3}], gen_rets:lookup(NewPid, "redink")),
    ?assertEqual(true, gen_rets:delete(NewPid, "redink")),

    ?assertEqual(not_found, gen_rets:update_counter(NewPid, "redink", {2, 1})),

    ?assertEqual(true, gen_rets:insert(NewPid, {"redink", 26}, {sec, 1})),
    timer:sleep(timer:seconds(2)),
    ?assertEqual(not_found, gen_rets:update_counter(NewPid, "redink", {2, 1})),
    ?assertEqual(true, gen_rets:delete(NewPid)),
    ok.

'18_update_counter_test/4'(Pid) ->
    gen_rets:update_counter(Pid, "redink", {2, 1}, {"redink", 26, "coder"}, {sec, 10}),
    ?assertEqual([{"redink", 27, "coder"}], gen_rets:lookup(Pid, "redink")),
    ?assertEqual(true, 10 - gen_rets:get_ttl(Pid, "redink") =< 1),
    ?assertEqual(true, gen_rets:delete(Pid, "redink")),

    gen_rets:update_counter(Pid, "redink", 1, {"redink", 26}, {sec, 10}),
    ?assertEqual([{"redink", 27}], gen_rets:lookup(Pid, "redink")),
    ?assertEqual(true, 10 - gen_rets:get_ttl(Pid, "redink") =< 1),
    ?assertEqual(true, gen_rets:delete(Pid, "redink")),

    gen_rets:update_counter(Pid, "redink", [{2, 1}, {3, 11, 10, 3}],
                            {"redink", 26, 1}, {sec, 10}),
    gen_rets:update_counter(Pid, "redink1", [{2, 1}, {3, 11, 10, 3}],
                            {"redink1", 26, 1}, {sec, 30}),
    ok = gen_server:call(Pid, exit),
    timer:sleep(timer:seconds(11)),
    {ok, NewPid} = gen_rets:start_link([{ets_table_name, '18_update_counter_test_4'},
                                        {new_ets_options, [named_table, public]},
                                        {name, '18_update_counter_test_4'},
                                        {persistence, aof}]),
    ?assertEqual([], gen_rets:lookup(NewPid, "redink")),
    ?assertEqual([{"redink1", 27, 3}], gen_rets:lookup(NewPid, "redink1")),
    ?assertEqual(true, gen_rets:delete(NewPid, "redink")),

    ?assertEqual(true, gen_rets:insert(NewPid, {"redink", 26}, {sec, 10})),
    gen_rets:update_counter(NewPid, "redink", 1, {"redink", 1}, {sec, 10}),
    ?assertEqual([{"redink", 27}], gen_rets:lookup(NewPid, "redink")),
    ?assertEqual(true, gen_rets:delete(NewPid, "redink")),

    ?assertEqual(true, gen_rets:insert(NewPid, {"redink", 26}, {sec, 1})),
    timer:sleep(timer:seconds(2)),
    ?assertEqual(not_found, gen_rets:update_counter(NewPid, "redink", {2, 1},
                                                    {"redink", 1}, {sec, 1})),
    ?assertEqual(true, gen_rets:delete(NewPid)),
    ok.

-endif.