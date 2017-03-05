#!/usr/bin/env escript
% bin/benchmark.erl 127.0.0.1 12345 2 2 /mnt/data/protostore.toc

-mode(compile).

main([Host, Port, NumProcs, Runtime, TocFile]) ->
    %% Read uuids so we know which keys to fetch.
    Uuids = read_toc(TocFile),
    io:format("Read toc with ~p entries~n", [length(Uuids)]),


    io:format("Starting ~p clients~n", [list_to_integer(NumProcs)]),
    Pids = lists:map(fun (_) ->
                             Sock = case gen_tcp:connect(Host, list_to_integer(Port), [binary, {active, false}]) of
                                        {ok, S} -> S;
                                        _ -> throw(could_not_connect)
                                    end,
                             spawn_link(fun () ->  hammer(Sock, Uuids) end)
                     end,
                     lists:seq(1, list_to_integer(NumProcs))),

    io:format("Running for ~p seconds~n", [list_to_integer(Runtime)]),
    timer:sleep(list_to_integer(Runtime) * 1000),

    [Pid ! {halt, self()} || Pid <- Pids],

    Timings = recv_all(Pids),
    io:format("ok~n"),

    Stats = bear:get_statistics(lists:flatten(Timings)),
    N = proplists:get_value(n, Stats),
    Percentiles = proplists:get_value(percentile, Stats),
    io:format("Number of requests: ~p~n", [N]),
    io:format("Reads per second: ~.2f~n", [N / list_to_integer(Runtime)]),

    [{50,P50},{75,P75},{90,P90},{95,P95},{99,P99},{999,P999}] = Percentiles,
    io:format("50th: ~pus 75th: ~pus 90th: ~pus 95th: ~pus 99th: ~pus 99.9: ~pus~n",
              [P50, P75, P90, P95, P99, P999]),

    ok.


recv_all(Pids) ->
    recv_all([], Pids).


recv_all(L, []) ->
    L;
recv_all(L, [Pid | Pids]) ->
    receive
        {Pid, timings, Timings} ->
            recv_all([Timings | L], Pids)
    after 1000 ->
            io:format("Could not receive timings from ~p~n", [Pid]),
            L
    end.


hammer(Sock, Uuids) ->
    Partition = trunc(rand:uniform() * length(Uuids)),
    A = lists:sublist(Uuids, Partition),
    B = lists:nthtail(Partition, Uuids),
    ReqId = 0,
    hammer(Sock, ReqId, A, B, []).

hammer(Sock, ReqId, [], L, Timings) ->
    hammer(Sock, ReqId, L, [], Timings);
hammer(Sock, ReqId, [Uuid | T], L, Timings) ->
    ok = gen_tcp:send(Sock, <<ReqId:32/unsigned-integer, Uuid/binary>>),
    {ElapsedUs, Result} = timer:tc(
                            fun () ->
                                    case gen_tcp:recv(Sock, 8, 5000) of
                                        {ok, <<ReqId:32/unsigned-integer,  Len:32/unsigned-integer>>} ->
                                            {ok, _Res} = gen_tcp:recv(Sock, Len),
                                            %%io:format("req id ~p, '~p'~n", [ReqId, Res]),
                                            ok;
                                        {error, _} = Error ->
                                            Error
                                    end
                            end),

    receive
        {halt, Pid} ->
            Pid ! {self(), timings, Timings}
    after 0 ->
            case Result of
                ok ->
                    hammer(Sock, ReqId+1, T, [Uuid | L], [ElapsedUs | Timings]);
                Error ->
                    throw({hammer_error, Error}),
                    {ok, Timings}
            end
    end.







read_toc(Path) ->
    io:format("Reading toc file from ~p~n", [Path]),
    {ok, F} = file:open(Path, [read, raw, binary]),
    do_read_toc(F, []).

do_read_toc(F, L) ->
    case file:read(F, 20) of
        {ok, <<Uuid:16/binary, _NumEntries:32/integer>>} ->
            do_read_toc(F, [Uuid | L]);
        eof ->
            L
    end.
