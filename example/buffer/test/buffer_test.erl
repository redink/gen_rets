-module(buffer_test).

-include_lib("eunit/include/eunit.hrl").

-export([buffer_test_help/3]).

buffer_test_() ->
    {setup,
     fun() ->
         ok
     end,
     fun(_) ->
         [ {"no cache test", timeout, 100,
            fun() ->
                {ok, _} = application:ensure_all_started(buffer),
                HelpEts = ets:new(help_ets, [public]),
                %% start a buffer instance
                {ok, _} = buffer:start_instance('one_buffer_instance',
                                                [{new_ets_options, [public]}]),
                TaskRefList =
                    [task:async(buffer, buffer_request,
                                ['one_buffer_instance', key,
                                 {?MODULE, buffer_test_help, [nocache, key, HelpEts]}])
                     || _ <- lists:seq(1, 10000)],
                ResList = [task:await(TaskRef) || TaskRef <- TaskRefList],
                ?assertEqual(1, erlang:length(lists:usort(ResList))),
                ?assertEqual([{key, 1}], ets:tab2list(HelpEts)),
                application:stop(buffer)
            end}
         , {"cache test", timeout, 100,
            fun() ->
                {ok, _} = application:ensure_all_started(buffer),
                {ok, _} = buffer:start_instance('one_buffer_instance',
                                                [{new_ets_options, [public]}]),
                HelpEts = ets:new(help_ets, [public]),
                TaskRefList =
                    [task:async(buffer, buffer_request,
                                ['one_buffer_instance', key,
                                 {?MODULE, buffer_test_help, [cache, key, HelpEts]}])
                     || _ <- lists:seq(1, 100)],
                [Result] = lists:usort([task:await(TaskRef) || TaskRef <- TaskRefList]),
                timer:sleep(1),
                ?assertEqual(Result,
                             buffer:buffer_request('one_buffer_instance', key,
                                                   {?MODULE, buffer_test_help,
                                                    [cache, key, HelpEts]})),
                application:stop(buffer)
            end}
         , {"cache test with fifo", timeout, 100,
            fun() ->
                CacheOptions = [{new_ets_options, [public]}, {max_size, 6}, {highwater_size, 6}],
                {ok, _} = application:ensure_all_started(buffer),
                {ok, Pid} = buffer:start_instance('one_buffer_instance', CacheOptions),
                IngEtsTable = maps:get(ingetstable, sys:get_state(Pid)),
                EtsTable    = maps:get(etstable   , sys:get_state(Pid)),
                HelpEts = ets:new(help_ets, [public]),
                F =
                    fun(Key) ->
                        TaskRefList =
                            [task:async(buffer, buffer_request,
                                        ['one_buffer_instance', Key,
                                         {?MODULE, buffer_test_help, [cache, Key, HelpEts]}])
                             || _ <- lists:seq(1, 100)],
                        ResList = lists:usort([task:await(TaskRef) || TaskRef <- TaskRefList]),
                        ?assertEqual(1, erlang:length(lists:usort(ResList))),
                        ?assertEqual([{Key, 1}], ets:lookup(HelpEts, Key)),
                        ok
                    end,
                TaskRefList =
                    [task:async(erlang, apply, [F, [K]]) || K <- lists:seq(1, 10)],
                timer:sleep(1000),
                ?assertEqual(10, ets:info(IngEtsTable, size)),
                [ok = task:await(TaskRef) || TaskRef <- TaskRefList],
                timer:sleep(5000),
                ?assertEqual(6, ets:info(EtsTable, size)),
                application:stop(buffer)
            end}
         ]
     end
    }.

buffer_test_help(Tag, Key, HelpEts) ->
    timer:sleep(2000),
    _ = ets:insert(HelpEts, {Key, 0}),
    _ = ets:update_counter(HelpEts, Key, 1),
    {X1, X2, X3} = os:timestamp(),
    {Tag, X1 * 1000000000000 + X2 * 1000000 + X3}.
