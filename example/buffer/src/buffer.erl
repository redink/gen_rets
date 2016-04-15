-module(buffer).

-export([start_instance/2, buffer_request/3]).

-define(INSTANCESUPNAME, buffer_instance_sup).

start_instance(SvrName, CacheOption) ->
    supervisor:start_child(?INSTANCESUPNAME, [SvrName, CacheOption]).

buffer_request(SvrName, Key, {Mod, Fun, Args}) ->
    buffer_request(SvrName, Key, {Mod, Fun, Args, 5000});
buffer_request(SvrName, Key, NoCacheMFA) ->
    case catch buffer_instance:get_cache(SvrName, Key) of
        [] ->
            buffer_request(buffer_instance:execute_ing(SvrName, Key),
                           Key, SvrName, NoCacheMFA);
        [{Key, ing, _, _}] ->
            {_, _, _, WaitTimeout} = NoCacheMFA,
            buffer_instance:add_wait_proc(SvrName, Key, self()),
            waiting_for_cache_ing(WaitTimeout);
        [{Key, ed, _, Result}] ->
            Result;
        _A ->
            {Mod, Fun, Args, _} = NoCacheMFA,
            {_, Result} = erlang:apply(Mod, Fun, Args),
            Result
    end.

buffer_request(execute, Key, SvrName, {Mod, Fun, Args, WaitTimeout}) ->
    case erlang:apply(Mod, Fun, Args) of
        {cache, Result} ->
            buffer_instance:execute_ed(SvrName, Key, Result);
        {_, Result} ->
            buffer_instance:execute_ed_no(SvrName, Key, Result);
        Result ->
            buffer_instance:execute_ed_no(SvrName, Key, Result)
    end,
    waiting_for_cache_ing(WaitTimeout);
buffer_request(waiting, _, _, {_, _, _, WaitTimeout}) ->
    waiting_for_cache_ing(WaitTimeout).

waiting_for_cache_ing(WaitTimeout) ->
    receive
        {'__buffer_return__', Result} ->
            Result
    after WaitTimeout ->
            timeout
    end.
