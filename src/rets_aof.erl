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
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(HIBERNATE_TIMEOUT, 10000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
start_link(Options) ->
    gen_server:start_link(?MODULE, [Options], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Options]) ->
    EtsTableName = gen_rets:get_list_item(ets_table_name, Options),
    ServerState  = gen_rets:get_list_item(server_state, Options),
    AOFRootDir   = application:get_env(gen_rets, aof_root_dir,
                                       "./aof_root_dir/"),
    LogDir = AOFRootDir ++ erlang:atom_to_list(EtsTableName) ++ "/",
    ok = filelib:ensure_dir(LogDir),
    case filelib:wildcard(LogDir ++ "*") of
        [] ->
            ok;
        _ ->
            ok = recover_table_data(LogDir, ServerState)
    end,
    LogTopic = gululog_topic:init(LogDir, [{cache_policy, minimum}]),
    {ok, #{ log_topic => LogTopic
          , log_dir   => LogDir
          , ets_table_name => EtsTableName}, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    {reply, ok, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_info({aof, _, clean_aof_log, _},
            #{ log_topic := LogTopic
             , log_dir   := LogDir} = State) ->
    ok = gululog_topic:close(LogTopic),
    [ok = file:delete(File) || File <- filelib:wildcard(LogDir ++ "*")],
    NewTopic = gululog_topic:init(LogDir, [{cache_policy, minimum}]),
    {noreply, State#{log_topic := NewTopic}, ?HIBERNATE_TIMEOUT};

handle_info({aof, EtsTableName, FunName, Args},
            #{ ets_table_name := EtsTableName
             , log_topic := LogTopic} = State) ->
    Header = erlang:atom_to_binary(EtsTableName, utf8),
    Body   = generate_body(FunName, Args),
    NewTopic = gululog_topic:append(LogTopic, Header, Body),
    {noreply, State#{log_topic := NewTopic}, ?HIBERNATE_TIMEOUT};

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

recover_table_data(LogDir, ServerState) ->
    ReadCur = gululog_r_cur:open(LogDir, 0),
    recover_table_data(gululog_r_cur:read(ReadCur), ReadCur, ServerState).

recover_table_data(eof, Cur, _) ->
    ok = gululog_r_cur:close(Cur),
    ok;
recover_table_data({NewReadCur, {_, _, _, B}}, _OldReadCur, ServerState) ->
    ok = execute_recover(B, ServerState),
    recover_table_data(gululog_r_cur:read(NewReadCur), NewReadCur, ServerState).

execute_recover(BinData, ServerState) ->
    Data    = process_body(BinData),
    FunName = gen_rets:get_list_item(funname, Data),
    Args    = gen_rets:get_list_item(args, Data),
    execute_recover(FunName, Args, ServerState).

execute_recover(insert, [OldNow, Objects, TTLOption], ServerState) ->
    case gen_rets:get_now() < gen_rets:get_ttl_time(TTLOption, OldNow) of
        true ->
            MainTable = maps:get(ets_main_table, ServerState),
            true = ets:insert(MainTable, Objects),
            ok   = gen_rets:set_ttl(OldNow, Objects, ServerState, TTLOption),
            ok;
        _ ->
            ok
    end;
execute_recover(insert_new, [OldNow, Objects, TTLOption], ServerState) ->
    case gen_rets:get_now() < gen_rets:get_ttl_time(TTLOption, OldNow) of
        true ->
            MainTable = maps:get(ets_main_table, ServerState),
            case ets:insert_new(MainTable, Objects) of
                true ->
                    ok = gen_rets:set_ttl(OldNow, Objects,
                                          ServerState, TTLOption),
                    ok;
                _ ->
                    ok
            end;
        _ ->
            ok
    end;
execute_recover(delete, [Key], ServerState) ->
    MainTable = maps:get(ets_main_table, ServerState),
    ok   = gen_rets:clean_other_table_via_key(no, Key, ServerState),
    true = ets:delete(MainTable, Key),
    ok;
execute_recover(delete_object, [Object], ServerState) ->
    NewETSOptions = maps:get(new_ets_options, ServerState),
    MainTable     = maps:get(ets_main_table , ServerState),
    Key = gen_rets:get_object_key(NewETSOptions, Object),
    case ets:lookup(Key, MainTable) of
        [] ->
            ignore;
        [_] ->
            ok   = gen_rets:clean_other_table_via_key(no, Key, ServerState),
            true = ets:delete_object(MainTable, Object);
        [_ | _] ->
            true = ets:delete_object(MainTable, Object)
    end,
    ok;
execute_recover(update_counter, [Key, UpdateOp], ServerState) ->
    MainTable = maps:get(ets_main_table, ServerState),
    case ets:lookup(MainTable, Key) of
        [] ->
            ignore;
        _ ->
            ets:update_counter(MainTable, Key, UpdateOp)
    end,
    ok;
execute_recover(update_counter,
                [OldNow, Key, UpdateOp, Default, TTLOption],
                ServerState) ->
    case gen_rets:get_now() < gen_rets:get_ttl_time(TTLOption, OldNow) of
        true ->
            MainTable = maps:get(ets_main_table, ServerState),
            case ets:lookup(MainTable, Key) of
                [] ->
                    true = ets:insert(MainTable, Default),
                    _  = ets:update_counter(MainTable, Key, UpdateOp),
                    ok = gen_rets:set_ttl(OldNow, Default,
                                          ServerState, TTLOption),
                    ok;
                _ ->
                    ets:update_counter(MainTable, Key, UpdateOp),
                    ok
            end;
        _ ->
            ok
    end;
execute_recover(reset_ttl, [OldNow, Key, Time], ServerState) ->
    TmpVar = gen_rets:get_ttl_time(Time, OldNow),
    case gen_rets:get_now() < TmpVar of
        true ->
            MetaTable = maps:get(ets_meta_table, ServerState),
            TTLTable  = maps:get(ets_ttl_table , ServerState),
            LRUTable  = maps:get(ets_lru_table , ServerState),
            case ets:lookup(MetaTable, Key) of
                [{Key, OldTTLTime, OldInsertTime, OldUpdateTime}] ->
                    TTLTime = TmpVar,
                    true = ets:delete(TTLTable, {OldTTLTime, Key}),
                    true = ets:delete(LRUTable, {OldUpdateTime, Key}),
                    true = ets:insert(TTLTable, {{TTLTime, Key}, nouse}),
                    true = ets:insert(LRUTable, {{OldNow, Key}, nouse}),
                    true = ets:insert(MetaTable, {Key, TTLTime, OldInsertTime, OldNow}),
                    ok;
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

generate_body(FunName, Args) ->
    erlang:term_to_binary([{funname, FunName}, {args, Args}]).

process_body(BinData) ->
    erlang:binary_to_term(BinData).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

rets_aof_test_() ->
    [ {"start_link 1",
        fun() ->
            {ok, Pid} = gen_rets:start_link([{ets_table_name, test_for_ets},
                                             {new_ets_options, [named_table]}]),
            gen_rets:insert(Pid, [{akey, avalue}, {bkey, bvalue}], {sec, 2}),
            timer:sleep(timer:seconds(3)),
            ?assertEqual([], gen_rets:lookup(Pid, akey)),
            ?assertEqual([], gen_rets:lookup(Pid, bkey)),
            ?assertEqual(true, gen_rets:delete(Pid)),
            ?assertEqual(undefined, ets:info(test_for_ets))
        end}
    , {"start_link 2",
        fun() ->
            {ok, Pid} = gen_rets:start([{name, test_process},
                                        {ets_table_name, test_for_ets},
                                        {new_ets_options, [named_table, public]},
                                        {persistence, aof}]),
            gen_rets:insert(Pid, [{akey, avalue}, {bkey, bvalue}], {sec, 100}),
            ok = gen_server:call(Pid, exit),
            {ok, Pid1} = gen_rets:start_link([{name, test_process},
                                              {ets_table_name, test_for_ets},
                                              {new_ets_options, [named_table, public]},
                                              {persistence, aof}]),
            ?assertEqual([{akey, avalue}], gen_rets:lookup(Pid1, akey)),
            ?assertEqual([{bkey, bvalue}], gen_rets:lookup(Pid1, bkey))
        end}
    ].

-endif.
