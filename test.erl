-module(test).

-export([test_sum/0,test_avgw/0,test_avgdw/0,test_grep/1,test_revIndex/0]).

test_sum() ->
        {ok, MR}  = mr:start(),
        {ok, Sum} = mr:job(MR,
                           3,
                           fun(X) -> X end,
                           fun(X, Acc) -> X + Acc end,
                           0,
                           lists:seq(1,10)),
        {ok, Fac} = mr:job(MR,
                           5,
                           fun(X) -> X end,
                           fun(X, Acc) -> X * Acc end,
                           1,
                           lists:seq(1,7)),
        mr:stop(MR),
        {Sum, Fac}.

test_avgw() ->
    part2:avgw().

test_avgdw() ->
    part2:avgdw().

test_grep(Word) ->
    part2:grep(Word).

test_revIndex() ->
    part2:revIndex().
