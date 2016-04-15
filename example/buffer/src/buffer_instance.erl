%%%-------------------------------------------------------------------
%%% @author redink
%%% @copyright (C) , redink
%%% @doc
%%%
%%% @end
%%% Created :  by redink
%%%-------------------------------------------------------------------
-module(buffer_instance).

-behaviour(gen_server).

%% API
-export([ start_link/2
        , get_cache/2
        , execute_ing/2
        , execute_ed/3
        , execute_ed_no/3
        , add_wait_proc/3
        ]).

%% nonblock API
-export([nonblock_get_cache/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(HIBERNATE_TIMEOUT, 10000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
get_cache(SvrName, Key) ->
    gen_server:call(SvrName, {get_cache, Key}).

execute_ing(SvrName, Key) ->
    gen_server:call(SvrName, {execute_ing, Key}).

execute_ed(SvrName, Key, ExecuteResult) ->
    gen_server:call(SvrName, {execute_ed, Key, ExecuteResult}).

execute_ed_no(SvrName, Key, ExecuteResult) ->
    gen_server:call(SvrName, {execute_ed_no, Key, ExecuteResult}).

add_wait_proc(SvrName, Key, WaitProc) ->
    gen_server:call(SvrName, {add_wait_proc, Key, WaitProc}).

start_link(SvrName, CacheOptions) ->
    gen_server:start_link({local, SvrName}, ?MODULE,
                          [CacheOptions], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([CacheOptions]) ->
    _ = erlang:process_flag(trap_exit, true),
    {ok, EtsPid} = gen_rets:start_link(CacheOptions),
    EtsTable     = gen_rets:get_ets(EtsPid),
    {ok, #{etspid => EtsPid, etstable => EtsTable}, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_call({get_cache, UNKey}, From,
            #{ etstable := EtsTable} = State) ->
    proc_lib:spawn_link(?MODULE, nonblock_get_cache,
                        [erlang:self(), EtsTable, UNKey, From]),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_call({execute_ing, UNKey}, {Pid, _},
            #{ etspid := EtsPid
             , etstable := EtsTable} = State) ->
    Key  = lz4_pack(UNKey),
    R =
        case ets:lookup(EtsTable, Key) of
            [] ->
                true = gen_rets:insert(EtsPid, {Key, ing, [Pid], []}),
                execute;
            [{Key, ing, WaitingList, _}] ->
                insert_waitinglist(EtsTable, Key, WaitingList, Pid),
                waiting
        end,
    {reply, R, State, ?HIBERNATE_TIMEOUT};

handle_call({execute_ed, UNKey, ExecuteResult}, _From,
            #{etspid := EtsPid} = State) ->
    Key = lz4_pack(UNKey),
    case catch gen_rets:lookup(EtsPid, Key) of
        [{Key, ing, WaitingList, _}] ->
            F =
                fun() ->
                    [erlang:send(WaitProc,
                                 {'__buffer_return__', ExecuteResult})
                     || WaitProc <- WaitingList]
                end,
            proc_lib:spawn(F),
            handle_execute_ed(EtsPid, Key, ExecuteResult),
            ok;
        _ ->
            ok
    end,
    {reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({execute_ed_no, UNKey, ExecuteResult}, _From,
            #{etspid := EtsPid} = State) ->
    Key = lz4_pack(UNKey),
    case catch gen_rets:lookup(EtsPid, Key) of
        [{Key, ing, WaitingList, _}] ->
            F =
                fun() ->
                    [erlang:send(WaitProc,
                                 {'__buffer_return__', ExecuteResult})
                     || WaitProc <- WaitingList]
                end,
            proc_lib:spawn(F),
            ok;
        _ ->
            ok
    end,
    gen_rets:delete(EtsPid, Key),
    {reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({add_wait_proc, UNKey, WaitProc}, _From,
            #{etstable := EtsTable} = State) ->
    block_add_wait_proc(EtsTable, UNKey, WaitProc),
    {reply, ok, State, ?HIBERNATE_TIMEOUT};

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
terminate(_Reason, #{etspid := EtsPid} = _State) ->
    true = gen_rets:delete(EtsPid),
    ok.

%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

nonblock_get_cache(ParentPid, EtsTable, UNKey, From) ->
    Key = lz4_pack(UNKey),
    Return =
        case catch ets:lookup(EtsTable, Key) of
            [{Data, ed, X3, X4}] ->
                [{lz4_unpack(Data), ed, X3, lz4_unpack(X4)}];
            [{Data, X2, X3, X4}] ->
                [{lz4_unpack(Data), X2, X3, X4}];
            Other ->
                Other
        end,
    gen_server:reply(From, Return),
    true = erlang:unlink(ParentPid),
    ok.

block_add_wait_proc(EtsTable, UNKey, WaitProc) ->
    Key = lz4_pack(UNKey),
    case ets:lookup(EtsTable, Key) of
        [] ->
            ok;
        [{Key, ed, _, ExecuteResult}] ->
            erlang:send(WaitProc, lz4_unpack(ExecuteResult));
        [{Key, ing, WaitingList, _}] ->
            insert_waitinglist(EtsTable, Key, WaitingList, WaitProc);
        _ ->
            ok
    end,
    ok.

insert_waitinglist(EtsTable, Key, WaitingList, WaitProc) ->
    case lists:member(WaitProc, WaitingList) of
        true ->
            ok;
        _ ->
            ets:update_element(EtsTable, Key,
                               {3, [WaitProc | WaitingList]})
    end,
    ok.

handle_execute_ed(EtsPid, Key, ExecuteResult) ->
    InsertObject = {Key, ed, [], lz4_pack(ExecuteResult)},
    true = gen_rets:insert(EtsPid, InsertObject, {hour, 12}),
    ok.

lz4_pack(Data) ->
    {ok, Pack} = lz4:pack(erlang:term_to_binary(Data)),
    Pack.

lz4_unpack(Data) ->
    {ok, BinData} = lz4:unpack(Data),
    erlang:binary_to_term(BinData).
