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
-export([nonblock_get_cache/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(HIBERNATE_TIMEOUT, 10000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
-spec get_cache(atom() | pid(), term()) -> [tuple()].
get_cache(SvrName, Key) ->
    gen_server:call(SvrName, {get_cache, Key}).

-spec execute_ing(atom() | pid(), term()) -> execute | waiting.
execute_ing(SvrName, Key) ->
    gen_server:call(SvrName, {execute_ing, Key}).

-spec execute_ed(atom() | pid(), term(), term()) -> ok.
execute_ed(SvrName, Key, ExecuteResult) ->
    gen_server:call(SvrName, {execute_ed, Key, ExecuteResult}).

-spec execute_ed_no(atom() | pid(), term(), term()) -> ok.
execute_ed_no(SvrName, Key, ExecuteResult) ->
    gen_server:call(SvrName, {execute_ed_no, Key, ExecuteResult}).

-spec add_wait_proc(atom() | pid(), term(), pid()) -> ok.
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
    {ok, IngEtsPid} = gen_rets:start_link([{expire_mode, none},
                                           {new_ets_options, [public]}]),
    IngEtsTable     = gen_rets:get_ets(IngEtsPid),
    {ok, #{ etspid => EtsPid
          , etstable => EtsTable
          , ingetspid => IngEtsPid
          , ingetstable => IngEtsTable}, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_call({get_cache, UNKey}, From,
            #{ etstable := EtsTable
             , ingetstable := IngEtsTable} = State) ->
    proc_lib:spawn_link(?MODULE, nonblock_get_cache,
                        [erlang:self(), EtsTable, IngEtsTable, UNKey, From]),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_call({execute_ing, UNKey}, {Pid, _},
            #{ ingetspid := IngEtsPid
             , ingetstable := IngEtsTable} = State) ->
    Key = lz4_pack(UNKey),
    R =
        case ets:lookup(IngEtsTable, Key) of
            [] ->
                true = gen_rets:insert(IngEtsPid, {Key, ing, [Pid], []}),
                execute;
            [{Key, ing, WaitingList, _}] ->
                insert_waitinglist(IngEtsTable, Key, WaitingList, Pid),
                waiting
        end,
    {reply, R, State, ?HIBERNATE_TIMEOUT};

handle_call({execute_ed, UNKey, ExecuteResult}, _From,
            #{ etspid := EtsPid
             , ingetspid := IngEtsPid} = State) ->
    Key = lz4_pack(UNKey),
    case catch gen_rets:lookup(IngEtsPid, Key) of
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
    gen_rets:delete(IngEtsPid, Key),
    {reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({execute_ed_no, UNKey, ExecuteResult}, _From,
            #{ingetspid := IngEtsPid} = State) ->
    Key = lz4_pack(UNKey),
    case catch gen_rets:lookup(IngEtsPid, Key) of
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
    gen_rets:delete(IngEtsPid, Key),
    {reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({add_wait_proc, UNKey, WaitProc}, _From,
            #{ingetstable := IngEtsTable} = State) ->
    block_add_wait_proc(IngEtsTable, UNKey, WaitProc),
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
terminate(_Reason, #{ etspid := EtsPid
                    , ingetspid := IngEtsPid} = _State) ->
    true = gen_rets:delete(EtsPid),
    true = gen_rets:delete(IngEtsPid),
    ok.

%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec nonblock_get_cache(pid(), ets:tid() | atom(), ets:tid() | atom(),
                         term(), {pid(), reference()}) -> ok.
nonblock_get_cache(ParentPid, EtsTable, IngEtsTable, UNKey, From) ->
    Key = lz4_pack(UNKey),
    Return = nonblock_get_cache(EtsTable, Key),
    IngReturn = nonblock_get_cache(IngEtsTable, Key),
    gen_server:reply(From, Return ++ IngReturn),
    true = erlang:unlink(ParentPid),
    ok.

-spec block_add_wait_proc(ets:tid() | atom(), term(), pid()) -> ok.
block_add_wait_proc(IngEtsTable, UNKey, WaitProc) ->
    Key = lz4_pack(UNKey),
    case ets:lookup(IngEtsTable, Key) of
        [] ->
            ok;
        [{Key, ed, _, ExecuteResult}] ->
            erlang:send(WaitProc, lz4_unpack(ExecuteResult));
        [{Key, ing, WaitingList, _}] ->
            insert_waitinglist(IngEtsTable, Key, WaitingList, WaitProc);
        _ ->
            ok
    end,
    ok.

-spec insert_waitinglist(ets:tid() | atom(), term(), list(), pid()) -> ok.
insert_waitinglist(IngEtsTable, Key, WaitingList, WaitProc) ->
    case lists:member(WaitProc, WaitingList) of
        true ->
            ok;
        _ ->
            ets:update_element(IngEtsTable, Key,
                               {3, [WaitProc | WaitingList]})
    end,
    ok.

-spec handle_execute_ed(pid() | atom(), term(), term()) -> ok.
handle_execute_ed(EtsPid, Key, ExecuteResult) ->
    InsertObject = {Key, ed, [], lz4_pack(ExecuteResult)},
    true = gen_rets:insert(EtsPid, InsertObject, {hour, 12}),
    ok.

-spec nonblock_get_cache(ets:tid() | atom(), term()) -> term().
nonblock_get_cache(EtsTable, Key) ->
    case catch ets:lookup(EtsTable, Key) of
        [{Data, ed, X3, X4}] ->
            [{lz4_unpack(Data), ed, X3, lz4_unpack(X4)}];
        [{Data, X2, X3, X4}] ->
            [{lz4_unpack(Data), X2, X3, X4}];
        Other ->
            Other
    end.

-spec lz4_pack(term()) -> binary().
lz4_pack(Data) ->
    {ok, Pack} = lz4:pack(erlang:term_to_binary(Data)),
    Pack.

-spec lz4_unpack(binary()) -> term().
lz4_unpack(Data) ->
    {ok, BinData} = lz4:unpack(Data),
    erlang:binary_to_term(BinData).
