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
    LogDir = filename:join(AOFRootDir, erlang:atom_to_list(EtsTableName)),
    ok = filelib:ensure_dir(LogDir),
    case filelib:wildcard("*.seg", LogDir) of
        [] ->
            ok;
        [OneFile] ->
            case filelib:file_size(filename:join(LogDir, OneFile)) of
                0 ->
                    ok;
                _ ->
                    ok = recover_table_data(LogDir, ServerState)
            end;
        _A ->
            ok = recover_table_data(LogDir, ServerState)
    end,
    LogTopic = gululog_topic:init(LogDir, [{cache_policy, minimum}]),
    {ok, #{ log_topic => LogTopic
          , log_dir   => LogDir
          , ets_table_name => EtsTableName}, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
handle_call({aof, EtsTableName, FunName, Args}, _From,
            #{ ets_table_name := EtsTableName
             , log_topic := LogTopic} = State) ->
    Header = erlang:atom_to_binary(EtsTableName, utf8),
    Body   = generate_body(FunName, Args),
    NewTopic = gululog_topic:append(LogTopic, Header, Body),
    {reply, ok, State#{log_topic := NewTopic}, ?HIBERNATE_TIMEOUT};

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
    [ok = file:delete(filename:join(LogDir, File))
     || File <- filelib:wildcard("*", LogDir)],
    ok = file:del_dir(LogDir),
    ok = filelib:ensure_dir(LogDir),
    NewTopic = gululog_topic:init(LogDir, [{cache_policy, minimum}]),
    {noreply, State#{log_topic := NewTopic}, ?HIBERNATE_TIMEOUT};

handle_info({aof, _, delete_aof_log, _},
            #{log_dir   := LogDir} = State) ->
    [ok = file:delete(filename:join(LogDir, File))
     || File <- filelib:wildcard("*", LogDir)],
    {stop, normal, State};

handle_info({aof, _, close_aof_log, _}, State) ->
    {stop, normal, State};

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
terminate(_Reason, #{log_topic := LogTopic} = _State) ->
    ok = gululog_topic:close(LogTopic),
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

execute_recover({Mod, Fun}, Args, ServerState) ->
    erlang:apply(Mod, Fun, [ServerState | Args]),
    ok.

generate_body(FunName, Args) ->
    lz4_pack([{funname, FunName}, {args, Args}]).

process_body(BinData) ->
    lz4_unpack(BinData).

lz4_pack(Data) ->
    {ok, Pack} = lz4:pack(erlang:term_to_binary(Data)),
    Pack.

lz4_unpack(Data) ->
    {ok, BinData} = lz4:unpack(Data),
    erlang:binary_to_term(BinData).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
