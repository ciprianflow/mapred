-module(worker).
-behaviour(gen_fsm).


% -compile(export_all).

-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).
-export([wait/2,wait/3,mapper/2,mapper/3,reducer/3]).



init(worker) ->
    {ok, wait, []}.

%worker events
wait({mapper, Data}, _State) ->
    % io:format("Mapper: ~p  - State: ~p ~n", [Data, State]),
    {next_state, mapper, Data};
wait({reducer, Data}, _State) ->
    % io:format("Reducer: ~p ~n", [Data]),
    {next_state, reducer, Data}.

%reduce data
wait({reduce, RedFun, Initial, Data}, _From, _StateData) ->
    % io:format("Reducing: ~p ~n", [Data]),

    Result = lists:foldl(RedFun, Initial, Data),

    {reply, Result, reducer, Result};
wait({reduce}, _From, _Data) ->
    {reply, [], wait, []}.

mapper({map, MapFun}, Data) ->

    MappedData = lists:map(MapFun, Data),

    % {reply, ok, wait, []}.
    {next_state, mapper, [MappedData]}.
mapper({reduce}, _From, Data) ->
    {reply, Data, wait, []}.


reducer({result}, _From, Data) ->
    {reply, Data, wait, []}.


handle_event({wait, _MapFun}, _StateName, _Data) ->
    % MappedData = lists:map(MapFun, Data),

    {next_state, wait, []};
    % {stop, other_cancelled, S};
handle_event({stop}, _StateName, _StateData) ->
    %stop workers

    {stop, normal, []}.
%% Unexpected allows to log unexpected messages
% unexpected(Msg, State) ->
%    io:format("~p received unknown event ~p while in state ~p~n",
%    [self(), Msg, State]).

terminate(_Reason, _StateName, _StateData) ->
   ok.

handle_info(_Info, StateName, StateData) ->
   {next_state, StateName, StateData}.
handle_sync_event(_Event, _From, StateName, StateData) ->
   {reply, ok, StateName, StateData}.
code_change(_OldVsn, StateName, StateData, _Extra) ->
   {ok, StateName, StateData}.
