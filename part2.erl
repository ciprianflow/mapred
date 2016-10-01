-module(part2).

-compile(export_all).


countw([]) -> 0;
countw([H | T]) ->
    {_,C} = H,
    C + countw(T).

wcount(Bin) ->
    {_,_, Words} = read_mxm:parse_track(Bin),
    countw(Words).

total_words() ->
    {_, Bin} = read_mxm:from_file("mxm_dataset_test.txt"),
    {ok, MR} = mr:start(),
    {ok, W} = mr:job(MR,10,
                        fun(Track) ->
                            wcount(Track)
                        end,
                        fun(X,Acc) ->
                            X + Acc
                        end,
                        0,
                        Bin),
    mr:stop(MR),
    W.


wavg(Track) ->
    {_,_, Words} = read_mxm:parse_track(Track),
    (countw(Words)/ length(Words)).

% average total number of words in a song.
avgw() ->
    {_, Bin} = read_mxm:from_file("mxm_dataset_test1.txt"),
    {ok, MR} = mr:start(),
    {ok, W} = mr:job(MR,10,
                        fun(Track) ->
                            wavg(Track)
                        end,
                        fun(X,Acc) ->
                            X + Acc
                        end,
                        0,
                        Bin),
    mr:stop(MR),
    W.

%Compute the average number of different words in a song
countdif([]) -> 0;
countdif([_ | T]) ->
    1 + countdif(T).
dwavg(Track) ->
    {_,_, Words} = read_mxm:parse_track(Track),
    (countdif(Words)/ countw(Words)).

avgdw() ->
    {_, Bin} = read_mxm:from_file("mxm_dataset_test1.txt"),
    {ok, MR} = mr:start(),
    {ok, W} = mr:job(MR,10,
                        fun(Track) ->
                            dwavg(Track)
                        end,
                        fun(X,Acc) ->
                            X + Acc
                        end,
                        0,
                        Bin),
    mr:stop(MR),
    W.


grep(Word) ->
    {Words, Bin} = read_mxm:from_file("mxm_dataset_test.txt"),

    Pos = string:str(Words, [Word]),
    case Pos of
        0 -> word_not_found;
        _ -> job(Pos, Bin)
    end.
job(Pos, Bin) ->
    {ok, MR} = mr:start(),
    {ok, W} = mr:job(MR,10,
                        fun(Track) ->
                            {MID,_, WordsTuple} = read_mxm:parse_track(Track),
                            case proplists:is_defined(Pos, WordsTuple) of
                                true -> [MID];
                                false -> []
                            end
                        end,
                        fun(X, Acc) ->
                            lists:append(X,Acc)
                        end,
                        [],
                        Bin),
    mr:stop(MR),
    W.

grep2(Word, Words, Bin) ->
    % {Words, Bin} = read_mxm:from_file("mxm_dataset_test.txt"),

    Pos = string:str(Words, [Word]),
    case Pos of
        0 -> word_not_found;
        _ -> job2(Pos, Bin)
    end.
job2(_, []) -> [];
job2(Pos, [Track | R]) ->
    % io:format("Track: ~p ~n",[Track]),
    case Track of
        [] -> [];
        _ -> {MID,_, WordsTuple} = read_mxm:parse_track(Track),
            case proplists:is_defined(Pos, WordsTuple) of
                true -> [MID |  job2(Pos, R)];
                false -> [], job2(Pos, R)
            end
        end.

% reverse index
revIndex() ->
    {Words, Bin} = read_mxm:from_file("mxm_dataset_test.txt"),

    {ok, MR} = mr:start(),
    {ok, RevIndex} = mr:job(MR,10,
                            fun(Word) ->
                                {Word, grep2(Word, Words, Bin)}
                                % {Word, part2:grep(Word)}
                            end,
                            fun({Word, Tracks}, Acc) ->
                                dict:append_list(Word, Tracks, Acc)
                            end,
                            dict:new(),
                            Words),
    mr:stop(MR),
    RevIndex.
