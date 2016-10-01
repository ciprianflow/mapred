%%%-------------------------------------------------------------------
%%% @author Ken Friis Larsen <kflarsen@diku.dk>
%%% @copyright (C) 2011-2015, Ken Friis Larsen
%%% @doc
%%%
%%% Functions for parsing the bag-of-words datasets from the
%%% musiXmatch dataset, the official lyrics collection for the Million
%%% Song Dataset, available at
%%% [http://labrosa.ee.columbia.edu/millionsong/musixmatch] (MXM).
%%%
%%% @end
%%% Last updated: Oct 2015 by Ken Friis Larsen <kflarsen@diku.dk>
%%%-------------------------------------------------------------------
-module(read_mxm).

%% API
-export([from_file/1,parse_track/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Read in a bag-of-words dataset from the MXM project.
%%
%% Returns a pair where the first component is a list of words, and
%% the second component is a list of binaries, one for each track. Use
%% `parse_track' to parse a binary track.
%%
%% @spec from_file(FileName :: string()) -> {[string()], [binary()]}
%%
%% @end
%%--------------------------------------------------------------------

from_file(FileName) ->
    {ok, Bin} = file:read_file(FileName),
    BLines = binary:split(Bin, <<$\n>>, [global,trim]),
    [WLine | Tracks] = lists:filter(fun (<<$#, _/binary>>) -> false;
                                        (_)                -> true end,
                                    BLines),
    Words = parse_words_line(WLine),
    {Words,Tracks}.


%%--------------------------------------------------------------------
%% @doc Parse a track from a binary on the form:
%%    `track_id, mxm_track_id, <word idx>:<cnt>, <word idx>:<cnt>, ...'
%%
%% Return a tuple where the first component is track_id, the second is
%% mxm_track_id, and the third is a list of pairs: word index and
%% count. Remember that word index starts at 1 (not zero).  See comment in
%% the start of the bag-of-words dataset for more detailed information.
%%
%% @spec parse_track(Track :: binary()) -> {binary(),binary(),[{integer(),integer()}]}
%%
%% @end
%%--------------------------------------------------------------------
parse_track(Track) ->
    [TrackId,MxmId| Rest] = binary:split(Track, <<$,>>, [global,trim]),
    {TrackId,MxmId,lists:map(fun parse_count/1, Rest)}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

parse_words_line(<<$%,WLine/binary>>) ->
    lists:map(fun binary_to_list/1, binary:split(WLine, <<$,>>, [global,trim])).


parse_count(CBin) ->
    [Widx,Cnt] = binary:split(CBin,<<$:>>),
    {binary_to_integer(Widx), binary_to_integer(Cnt)}.
