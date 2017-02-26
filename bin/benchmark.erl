#!/usr/bin/env escript

main([Host, Port]) ->
    Sock = case gen_tcp:connect(Host, list_to_integer(Port), [binary, {active, false}]) of
               {ok, S} -> S;
               _ -> throw(could_not_connect)
           end,

    %% Read uuids so we know which keys to fetch.

    Toc = read_toc("/tmp/protostore.toc"),
    io:format("Read toc with ~p entries", [maps:size(Toc)]),


    maps:map(fun (Key, NumEntries) ->
                     ok = gen_tcp:send(Sock, <<123:32/unsigned-integer, Key/binary>>),
                     recv_loop(Sock)
             end,
             Toc),

    ok.



recv_loop(Sock) ->
    case gen_tcp:recv(Sock, 8, 5000) of
        {ok, <<ReqId:32/unsigned-integer,  Len:32/unsigned-integer>>} ->
            {ok, Res} = gen_tcp:recv(Sock, Len),
            io:format("req id ~p, '~p'~n", [ReqId, Res]),
            recv_loop(Sock);
        Error ->
            io:format("got error ~p~n", [Error])
    end.



read_toc(Path) ->
    {ok, F} = file:open(Path, [read, raw, binary]),
    do_read_toc(F, #{}).

do_read_toc(F, M) ->
    case file:read(F, 20) of
        {ok, <<Uuid:16/binary, NumEntries:32/integer>>} ->
            do_read_toc(F, maps:put(Uuid, NumEntries, M));
        eof ->
            M
    end.
