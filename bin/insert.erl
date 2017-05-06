#!/usr/bin/env escript
%% Localhost
%% bin/insert.erl 127.0.0.1 12345 10 10 /mnt/data/


-mode(compile).

-include_lib("kernel/include/file.hrl").

main([Host, PortString, NumProcessesString, RuntimeString, DataDir]) ->
    Port = list_to_integer(PortString),
    NumProcesses = list_to_integer(NumProcessesString),
    Runtime = list_to_integer(RuntimeString),

    Toc = read_toc(DataDir),

    DummyData = iolist_to_binary([<<(rand:uniform(255)-1):8/integer>> || _ <- lists:seq(0, max_len(Toc))]),
    %%DummyData = iolist_to_binary([<<I:8/unsigned-integer>> || I <- lists:seq(0, max_len(Toc))]),

    Pids = lists:map(fun (_) ->
                             spawn_link(fun () ->
                                                Sock = case gen_tcp:connect(Host, Port, [binary, {active, false}]) of
                                                           {ok, S} -> S;
                                                           _ -> throw(could_not_connect)
                                                       end,
                                                hammer(Sock, Toc, DummyData) end)
                     end,
                     lists:seq(1, NumProcesses)),
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
    io:format("Runtime: ~p s~n", [Runtime]),
    io:format("Total requests: ~p~n", [N]),
    io:format("Avg writes per second: ~.2f~n", [N / Runtime]),
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


hammer(Sock, Toc, DummyData) ->
    {Uuids, _} = Toc,
    Position = trunc(rand:uniform() * (byte_size(Uuids) div 16)),
    MaxPos = byte_size(Uuids) div 16,

    ReqId = 0,

    receive hammertime -> ok end,
    io:format("Process ~p: It's hammertime!~n", [self()]),
    hammer(Sock, ReqId, Toc, Position, MaxPos, DummyData, [], []).


hammer(Sock, ReqId, Toc, Pos, MaxPos, DummyData, Timings, Lens) ->
    receive
        {halt, Pid} ->
            Pid ! {self(), results, Timings, Lens}
    after 0 ->
            {Uuids, ValueLens} = Toc,
            Uuid = binary:part(Uuids, Pos*16, 16),
            <<BodyLen:16/little-integer>> = binary:part(ValueLens, Pos*2, 2),
            Body = binary:part(DummyData, 0, BodyLen),
            Len = 1+16+4+byte_size(Body),

            Req = <<Len:32/unsigned-integer, "W", Uuid/binary, ReqId:32/unsigned-integer, Body/binary>>,
            Start = erlang:convert_time_unit(erlang:system_time(), native, microsecond),
            ok = gen_tcp:send(Sock, Req),
            case gen_tcp:recv(Sock, 0, 1000) of
                {ok, <<ReqId:32/unsigned-integer, 0:16/unsigned-integer>>} ->
                    End = erlang:convert_time_unit(erlang:system_time(), native, microsecond),
                    ElapsedUs = End - Start,

                    %% Read our write
                    ok = gen_tcp:send(Sock, <<(1+16+4):32/unsigned-integer, "R", Uuid/binary, ReqId:32/unsigned-integer>>),
                    case gen_tcp:recv(Sock, 6, 1000) of
                        {ok, <<ReqId:32/unsigned-integer, ResponseLen:16/unsigned-integer>>} ->
                            {ok, ResponseBody} = gen_tcp:recv(Sock, ResponseLen, 1000),

                            %%io:format("ResponseLen:~p BodyLen:~p~n", [ResponseLen, BodyLen]),
                            %%io:format("ResponseBody =:= ExpectedBody: ~p~n", [ResponseBody =:= Body]),
                            case ResponseBody =:= Body of
                                true ->
                                    ok;
                                false ->
                                    io:format("Read our write. Expected~n~p~nReceived~n~p~n",
                                              [Body, ResponseBody]),
                                    throw(stop)
                            end
                    end,

                    NewPos = rand:uniform(MaxPos)-1,
                    %%timer:sleep(1000),
                    hammer(Sock, ReqId+1, Toc, NewPos, MaxPos, DummyData, [ElapsedUs | Timings], [BodyLen | Lens]);
                Any ->
                    io:format("Got unexpected response ~p~n", [Any]),
                    throw(unexpected_response)
            end
    end.


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


max_len({_Uuid, Lens}) ->
    do_max_len(0, Lens).

do_max_len(A, <<>>) ->
    A;
do_max_len(A, <<B:16/little-integer, Rest/binary>>) ->
    do_max_len(max(A, B), Rest).
