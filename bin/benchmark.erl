#!/usr/bin/env escript
%% Localhost
%% bin/benchmark.erl protostore 127.0.0.1 12345 2 2 /mnt/data/protostore.toc
%% bin/benchmark.erl redis 127.0.0.1 6379 2 2 /mnt/data/protostore.toc
%%
%% i3
%% bin/benchmark.erl protostore 172.18.230.186 12345 2 2 /mnt/data/protostore.toc
%%
%% r3
%% bin/benchmark.erl redis 172.18.229.112 6379 2 2 redis.toc

-mode(compile).

-include_lib("kernel/include/file.hrl").

main([ServerTypeString, Host, PortString, NumProcsString, RuntimeString, TocFile]) ->
    ServerType = list_to_atom(ServerTypeString),
    Port = list_to_integer(PortString),
    NumProcs = list_to_integer(NumProcsString),
    Runtime = list_to_integer(RuntimeString),

    %% Read uuids so we know which keys to fetch.
    Toc = read_toc(TocFile),
    io:format("Read toc with ~p entries~n", [byte_size(Toc) div 20]),


    io:format("Starting ~p clients~n", [NumProcs]),
    Pids = lists:map(fun (_) ->
                             Sock = case gen_tcp:connect(Host, Port, [binary, {active, false}]) of
                                        {ok, S} -> S;
                                        _ -> throw(could_not_connect)
                                    end,
                             spawn_link(fun () -> hammer(ServerType, Sock, Toc) end)
                     end,
                     lists:seq(1, NumProcs)),

    io:format("Running for ~p seconds~n", [Runtime]),
    [Pid ! hammertime || Pid <- Pids],
    timer:sleep(Runtime * 1000),

    [Pid ! {halt, self()} || Pid <- Pids],
    io:format("============================~n"),

    {Timings, Lens} = recv_all(Pids),

    BytesTransferred = lists:sum(lists:flatten(Lens)),

    Stats = bear:get_statistics(lists:flatten(Timings)),
    N = proplists:get_value(n, Stats),
    Percentiles = proplists:get_value(percentile, Stats),
    io:format("Concurrent clients: ~p~n", [NumProcs]),
    io:format("Runtime: ~p s~n", [Runtime]),
    io:format("Total requests: ~p~n", [N]),
    io:format("Avg rps: ~.2f~n", [N / Runtime]),
    io:format("Bytes transferred: ~.2f MB~n", [BytesTransferred / 1024 / 1024]),
    io:format("Bytes per second: ~.2f MB~n", [(BytesTransferred / 1024 / 1024) / Runtime]),

    [{50,P50},{75,P75},{90,P90},{95,P95},{99,P99},{999,P999}] = Percentiles,
    io:format("Roundtrip latencies: 50th: ~pus 75th: ~pus 90th: ~pus 95th: ~pus 99th: ~pus 99.9: ~pus~n",
              [P50, P75, P90, P95, P99, P999]),
    ok.


recv_all(Pids) ->
    recv_all([], [], Pids).


recv_all(T, L, []) ->
    {T, L};
recv_all(T, L, [Pid | Pids]) ->
    receive
        {Pid, results, Timings, Lens} ->
            recv_all([Timings | T], [Lens | L], Pids)
    after 10000 ->
            io:format("Could not receive timings from ~p~n", [Pid]),
            {T, L}
    end.


hammer(ServerType, Sock, Toc) ->
    Position = trunc(rand:uniform() * (byte_size(Toc) div 20)),
    MaxPos = byte_size(Toc) div 20,
    ReqId = 0,

    receive hammertime -> ok end,
    io:format("Process ~p: It's hammertime!~n", [self()]),
    hammer(ServerType, Sock, ReqId, Toc, Position, MaxPos, [], []).

hammer(ServerType, Sock, ReqId, Toc, MaxPos, MaxPos, Timings, Lens) ->
    hammer(ServerType, Sock, ReqId, Toc, 0, MaxPos, Timings, Lens);
hammer(protostore, Sock, ReqId, Toc, Pos, MaxPos, Timings, Lens) ->
    Uuid = binary:part(Toc, Pos*20, 16),

    %% Time the full roundtrip, including sending the request and
    %% receiveng the full response.
    {ElapsedUs, Result} = timer:tc(
                            fun () ->
                                    ok = gen_tcp:send(Sock, <<ReqId:32/unsigned-integer, Uuid/binary>>),

                                    case gen_tcp:recv(Sock, 8, 5000) of
                                        {ok, <<ReqId:32/unsigned-integer,  0:32/unsigned-integer>>} ->
                                            throw(bad_response_length);
                                        {ok, <<ReqId:32/unsigned-integer,  Len:32/unsigned-integer>>} ->
                                            {ok, _Res} = gen_tcp:recv(Sock, Len, 5000),
                                            {ok, Len};
                                        {error, timeout} ->
                                            io:format("Process ~p: timeout waiting for req id ~p, uuid ~p~n", [self(), ReqId, Uuid]),
                                            throw(timeout);
                                        Error ->
                                            Error
                                    end
                            end),
    receive
        {halt, Pid} ->
            ok = gen_tcp:close(Sock),
            Pid ! {self(), results, Timings, Lens}
    after 0 ->
            case Result of
                {ok, Len} ->
                    hammer(protostore, Sock, ReqId+1, Toc, Pos+1, MaxPos, [ElapsedUs | Timings], [Len | Lens]);
                Error ->
                    throw({hammer_error, ReqId, Error})
            end
    end;

hammer(redis, Sock, ReqId, Toc, Pos, MaxPos, Timings, Lens) ->
    Uuid = binary:part(Toc, Pos*20, 16),

    %% Time the full roundtrip, including sending the request and
    %% receiveng the full response.
    {ElapsedUs, Result} = timer:tc(
                            fun () ->
                                    Req = [<<"*2\r\n">>, to_bulk(<<"GET">>), to_bulk(Uuid)],
                                    ok = gen_tcp:send(Sock, Req),

                                    case gen_tcp:recv(Sock, 0, 5000) of
                                        {ok, <<"$-1\r\n">>} ->
                                            throw(key_not_found);

                                        {ok, <<$$, LenString:4/binary, "\r\n", Body/binary>>} ->
                                            Len = list_to_integer(binary_to_list(LenString)),
                                            case Len =:= byte_size(Body)-2 of
                                                true ->
                                                    ok;
                                                false ->
                                                    {ok, _} = gen_tcp:recv(Sock, Len-byte_size(Body)+2, 5000)
                                            end,

                                            {ok, Len};
                                        {ok, Other} ->
                                            io:format("Got unexpected response: ~p~n", [Other]),
                                            throw(stop);
                                        {error, timeout} ->
                                            io:format("Process ~p: timeout waiting for req id ~p, uuid ~p~n", [self(), ReqId, Uuid]),
                                            throw(timeout);
                                        Error ->
                                            Error
                                    end
                            end),
    receive
        {halt, Pid} ->
            ok = gen_tcp:close(Sock),
            Pid ! {self(), results, Timings, Lens}
    after 0 ->
            case Result of
                {ok, Len} ->
                    hammer(redis, Sock, ReqId+1, Toc, Pos+1, MaxPos, [ElapsedUs | Timings], [Len | Lens]);
                Error ->
                    throw({hammer_error, ReqId, Error})
            end
    end.


to_bulk(B) when is_binary(B) ->
    [<<$$>>, integer_to_list(iolist_size(B)), <<"\r\n">>, B, <<"\r\n">>].


read_toc(Path) ->
    io:format("Reading toc file from ~p~n", [Path]),

    {ok, FileInfo} = file:read_file_info(Path),
    TotalSize = FileInfo#file_info.size,
    io:format("Toc is ~p bytes, ~p entries~n", [TotalSize, TotalSize div 20]),

    {ok, F} = file:open(Path, [read, raw, binary]),

    Toc = read_chunk(F, min(10000000*20, TotalSize), TotalSize, 0, []),
    iolist_to_binary(Toc).


read_chunk(F, ChunkSize, TotalSize, Position, Toc) ->
    MyChunk = min(ChunkSize, TotalSize),
    case MyChunk > 0 of
        true ->
            {ok, Bytes} = file:read(F, MyChunk),
            {ok, NewPos} = file:position(F, Position+MyChunk),
            read_chunk(F, ChunkSize, TotalSize-MyChunk, NewPos, [Bytes, Toc]);
        false ->
            Toc
    end.
