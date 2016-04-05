-module(bench).

-compile(export_all).
-define(TOTALN, 1000000).

start() ->
    io:format("~n~n"),
    ok = write_ets(),
    ok = write_rets_no_persistence(),
    ok = write_rets_trigger_lru_no_persistence(),
    ok = write_rets_with_persistence(),
    init:stop(),
    ok.

write_ets() ->
    Table  = ets:new(t, [named_table, public, {write_concurrency, true}]),
    TotalN = ?TOTALN,
    io:format("============ begin write ets table ===========~n"),
    Before = os:timestamp(),
    TaskRefList =
        [begin
            AllN = TotalN div 5,
            F = fun() ->
                [ets:insert(Table, {X, X}) || X <- lists:seq(AllN * (I - 1) + 1, AllN * I)],
                ok
            end,
            task:async(F)
         end || I <- lists:seq(1, 5)],
    [ok = task:await(T, infinity) || T <- TaskRefList],
    After = os:timestamp(),
    io:format(" time used : ~p~n", [timer:now_diff(After, Before)]),
    io:format(" write ets : ~p/s~n ", [TotalN / timer:now_diff(After, Before) * ?TOTALN]),
    ets:delete(Table),
    io:format("============= end write ets table ============~n~n"),
    ok.

write_rets_no_persistence() ->
    {ok, Pid} = gen_rets:start_link([{ets_table_name, test_for_ets},
                                     {new_ets_options, [named_table]},
                                     {max_size, 2000000},
                                     {highwater_size, 1900000}]),
    TotalN = ?TOTALN,
    io:format("============ begin write rets no persistence ===========~n"),
    Before = os:timestamp(),
    TaskRefList =
        [begin
            AllN = TotalN div 5,
            F = fun() ->
                [gen_rets:insert(Pid, {X, X}, {hour, 1})
                 || X <- lists:seq(AllN * (I - 1) + 1, AllN * I)],
                ok
            end,
            task:async(F)
         end || I <- lists:seq(1, 5)],
    [ok = task:await(T, infinity) || T <- TaskRefList],
    After = os:timestamp(),
    io:format(" time used : ~p~n", [timer:now_diff(After, Before)]),
    io:format(" write ets : ~p/s~n ", [TotalN / timer:now_diff(After, Before) * ?TOTALN]),
    true = gen_rets:delete(Pid),
    io:format("============= end write rets no persistence ============~n~n"),
    ok.

write_rets_trigger_lru_no_persistence() ->
    {ok, Pid} = gen_rets:start_link([{ets_table_name, test_for_ets},
                                     {new_ets_options, [named_table]},
                                     {max_size, 900000},
                                     {highwater_size, 700000}]),
    TotalN = ?TOTALN,
    io:format("============ begin write rets trigger lru no persistence ===========~n"),
    Before = os:timestamp(),
    TaskRefList =
        [begin
            AllN = TotalN div 5,
            F = fun() ->
                [gen_rets:insert(Pid, {X, X}, {hour, 1})
                 || X <- lists:seq(AllN * (I - 1) + 1, AllN * I)],
                ok
            end,
            task:async(F)
         end || I <- lists:seq(1, 5)],
    [ok = task:await(T, infinity) || T <- TaskRefList],
    After = os:timestamp(),
    io:format(" time used : ~p~n", [timer:now_diff(After, Before)]),
    io:format(" write ets : ~p/s~n ", [TotalN / timer:now_diff(After, Before) * ?TOTALN]),
    true = gen_rets:delete(Pid),
    io:format("============= end write rets trigger lru no persistence ============~n~n"),
    ok.

write_rets_with_persistence() ->
    {ok, Pid} = gen_rets:start_link([{ets_table_name, 'test_for_rets'},
                                     {new_ets_options, [named_table, public]},
                                     {name, 'test_for_rets'},
                                     {persistence, aof},
                                     {max_size, 2000000},
                                     {highwater_size, 1900000}]),
    TotalN = ?TOTALN,
    io:format("============ begin write rets with persistence ===========~n"),
    Before = os:timestamp(),
    TaskRefList =
        [begin
            AllN = TotalN div 5,
            F = fun() ->
                [gen_rets:insert(Pid, {X, X}, {hour, 1})
                 || X <- lists:seq(AllN * (I - 1) + 1, AllN * I)],
                ok
            end,
            task:async(F)
         end || I <- lists:seq(1, 5)],
    [ok = task:await(T, infinity) || T <- TaskRefList],
    After = os:timestamp(),
    io:format(" time used : ~p~n", [timer:now_diff(After, Before)]),
    io:format(" write ets : ~p/s~n ", [TotalN / timer:now_diff(After, Before) * ?TOTALN]),
    true = gen_rets:delete(Pid),
    io:format("============= end write rets with persistence ============~n~n"),
    ok.