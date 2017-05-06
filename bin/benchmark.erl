#!/usr/bin/env escript
%% Localhost
%% bin/benchmark.erl protostore 127.0.0.1 12345 2 2 10 /mnt/data/protostore.toc
%% bin/benchmark.erl redis 127.0.0.1 6379 2 2 10 /mnt/data/protostore.toc
%%
%% i3
%% bin/benchmark.erl protostore 172.18.246.107 12345 2 2 10 /mnt/data/protostore.toc
%%
%% r4
%% bin/benchmark.erl redis 172.18.246.29 6379 2 2 10 redis.toc

-mode(compile).

-include_lib("kernel/include/file.hrl").

main([ServerTypeString, Host, PortString, NumProcsString, RuntimeString, MaxInflightString, DataDir]) ->
    ServerType = list_to_atom(ServerTypeString),
    Port = list_to_integer(PortString),
    NumProcs = list_to_integer(NumProcsString),
    Runtime = list_to_integer(RuntimeString),
    MaxInflight = list_to_integer(MaxInflightString),

    %% Read uuids so we know which keys to fetch.
    Toc = read_toc(DataDir),
    %%io:format("Read toc with ~p entries~n", [byte_size(element(1, Toc)) div 20]),


    io:format("Starting ~p clients~n", [NumProcs]),
    Pids = lists:map(fun (_) ->
                             spawn_link(fun () ->
                                                Sock = case gen_tcp:connect(Host, Port, [binary, {active, true}]) of
                                                           {ok, S} -> S;
                                                           _ -> throw(could_not_connect)
                                                       end,
                                                hammer(ServerType, Sock, MaxInflight, Toc) end)
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


hammer(ServerType, Sock, MaxInflight, Toc) ->
    {Uuids, _} = Toc,
    Position = trunc(rand:uniform() * (byte_size(Uuids) div 16)),
    MaxPos = byte_size(Uuids) div 16,
    ReqId = 0,
    InflightReqs = #{},

    receive hammertime -> ok end,
    io:format("Process ~p: It's hammertime!~n", [self()]),
    hammer(ServerType, Sock, ReqId, InflightReqs, MaxInflight, Toc, Position, MaxPos, [], []).


hammer(ServerType, Sock, ReqId, InflightReqs, MaxInflight, Toc, MaxPos, MaxPos, Timings, Lens) ->
    %% Wrap position around, start at 0 again
    hammer(ServerType, Sock, ReqId, InflightReqs, MaxInflight, Toc, 0, MaxPos, Timings, Lens);

hammer(protostore, Sock, ReqId, InflightReqs, MaxInflight, Toc, Pos, MaxPos, Timings, Lens) ->

    receive
        {halt, Pid} ->
            {_, NewTimings, NewLens} = recv(Sock, InflightReqs, Timings, Lens),
            ok = gen_tcp:close(Sock),
            Pid ! {self(), results, NewTimings, NewLens}
    after 0 ->
            case maps:size(InflightReqs) =:= MaxInflight of
                true ->
                    {NewInflightReqs, NewTimings, NewLens} = block_recv(Sock, InflightReqs, Timings, Lens),
                    hammer(protostore, Sock, ReqId, NewInflightReqs, MaxInflight, Toc, Pos, MaxPos, NewTimings, NewLens);

                false ->
                    {Uuids, _ValueLens} = Toc,
                    Uuid = binary:part(Uuids, Pos*16, 16),
                    Now = erlang:convert_time_unit(erlang:system_time(), native, microsecond),

                    ok = gen_tcp:send(Sock, <<(1+16+4):32/unsigned-integer, "R", Uuid/binary, ReqId:32/unsigned-integer>>),
                    InflightReqs1 = maps:put(ReqId, Now, InflightReqs),

                    {InflightReqs2, NewTimings, NewLens} = recv(Sock, InflightReqs1, Timings, Lens),
                    NewPos = rand:uniform(MaxPos),
                    hammer(protostore, Sock, ReqId+1, InflightReqs2, MaxInflight, Toc, NewPos, MaxPos, NewTimings, NewLens)
            end
    end;



hammer(redis, Sock, ReqId, InflightReqs, MaxInflightReqs, Toc, Pos, MaxPos, Timings, Lens) ->
    Uuid = binary:part(Toc, Pos*16, 16),

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
                    hammer(redis, Sock, ReqId+1, InflightReqs, MaxInflightReqs, Toc, Pos+1, MaxPos, [ElapsedUs | Timings], [Len | Lens]);
                Error ->
                    throw({hammer_error, ReqId, Error})
            end
    end.





block_recv(Sock, Inflight, Timings, Lens) ->
    block_recv(Sock, <<>>, Inflight, Timings, Lens).

block_recv(Sock, Partial, Inflight, Timings, Lens) ->
    receive
        {tcp, Sock, Data} ->
            parse(Sock, <<Partial/binary, Data/binary>>, Inflight, Timings, Lens)
    end.

%% Receive as many responses are available without blocking
recv(Sock, Inflight, Timings, Lens) ->
    recv(Sock, <<>>, Inflight, Timings, Lens).

recv(Sock, Partial, Inflight, Timings, Lens) ->
    receive
        {tcp, Sock, Data} ->
            parse(Sock, <<Partial/binary, Data/binary>>, Inflight, Timings, Lens)
    after 0 ->
            {Inflight, Timings, Lens}
    end.


parse(Sock, Data, Inflight, Timings, Lens) ->
    case Data of
        <<>> ->
            {Inflight, Timings, Lens};

        <<_:32/unsigned-integer,  0:16/unsigned-integer, _/binary>> ->
            io:format("bad data: ~p~n", [Data]),
            throw(bad_response);

        <<ReqId:32/unsigned-integer, Len:16/unsigned-integer, Body/binary>> ->
            case byte_size(Body) >= Len of
                true ->
                    <<_ResponseBody:Len/binary, Rest/binary>> = Body,
                    End = erlang:convert_time_unit(erlang:system_time(), native, microsecond),
                    {Start, NewInflight} = maps:take(ReqId, Inflight),
                    ElapsedUs = End - Start,
                    parse(Sock, Rest, NewInflight, [ElapsedUs | Timings], [Len | Lens]);
                false ->
                    block_recv(Sock, Data, Inflight, Timings, Lens)
            end;

        _ ->
            block_recv(Sock, Data, Inflight, Timings, Lens)
    end.




to_bulk(B) when is_binary(B) ->
    [<<$$>>, integer_to_list(iolist_size(B)), <<"\r\n">>, B, <<"\r\n">>].


read_toc(DataDir) ->
    io:format("Reading Table of Contents from ~p~n", [DataDir]),

    UuidPath = filename:join([DataDir, "protostore.toc.uuids"]),
    LensPath = filename:join([DataDir, "protostore.toc.lengths"]),

    {ok, UuidFileInfo} = file:read_file_info(UuidPath),
    {ok, LensFileInfo} = file:read_file_info(LensPath),
    UuidTotalSize = UuidFileInfo#file_info.size,
    LensTotalSize = LensFileInfo#file_info.size,
    io:format("Uuid table of contents is ~p bytes, ~p entries~n", [UuidTotalSize, UuidTotalSize div 16]),

    {ok, UuidF} = file:open(UuidPath, [read, raw, binary]),
    {ok, LensF} = file:open(LensPath, [read, raw, binary]),

    Uuids = read_chunk(UuidF, min(10000000*16, UuidTotalSize), UuidTotalSize, 0, []),
    Lens = read_chunk(LensF, min(10000000*8, LensTotalSize), LensTotalSize, 0, []),
    {iolist_to_binary(Uuids), iolist_to_binary(Lens)}.


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
