-module(query_cache).

-export([start_instance/2, request_query/3]).

-define(INSTANCESUPNAME, query_cache_instance_sup).

start_instance(SvrName, CacheOption) ->
    supervisor:start_child(?INSTANCESUPNAME, [SvrName, CacheOption]).

request_query(SvrName, Key, {Mod, Fun, Args}) ->
    request_query(SvrName, Key, {Mod, Fun, Args, 5000});
request_query(SvrName, Key, NoCacheMFA) ->
    case catch query_cache_instance:get_cache(SvrName, Key) of
        [] ->
            request_query(query_cache_instance:query_ing(SvrName, Key),
                          Key, SvrName, NoCacheMFA);
        [{Key, ing, _, _}] ->
            {_, _, _, WaitTimeout} = NoCacheMFA,
            query_cache_instance:add_wait_proc(SvrName, Key, self()),
            waiting_for_cache_ing(WaitTimeout);
        [{Key, ed, _, Result}] ->
            Result;
        _A ->
            {Mod, Fun, Args, _} = NoCacheMFA,
            {_, Result} = erlang:apply(Mod, Fun, Args),
            Result
    end.

request_query(execute, Key, SvrName, {Mod, Fun, Args, WaitTimeout}) ->
    case erlang:apply(Mod, Fun, Args) of
        {cache, Result} ->
            query_cache_instance:query_ed(SvrName, Key, Result),
            waiting_for_cache_ing(WaitTimeout);
        {_, Result} ->
            query_cache_instance:query_ed_no(SvrName, Key, Result),
            waiting_for_cache_ing(WaitTimeout)
    end;
request_query(waiting, _, _, {_, _, _, WaitTimeout}) ->
    waiting_for_cache_ing(WaitTimeout).

waiting_for_cache_ing(WaitTimeout) ->
    receive
        {'__buffer_return__', Result} ->
            Result
    after WaitTimeout ->
            timeout
    end.

