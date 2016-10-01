-module(mr).
-behaviour(gen_fsm).

%export API functions
-export([start/0,job/6,advanced_job/6,stop/1]).
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

start() ->
    gen_fsm:start_link(master, master, []).


job(Pid, NumWork, MapFun, RedFun, Initial, Data) ->
    % io:format("Pid"),
    gen_fsm:sync_send_event(Pid, {init, NumWork, Data}),

    %start mapping
    gen_fsm:sync_send_event(Pid, {map, NumWork, MapFun}),

    %send data to reducers
    gen_fsm:sync_send_event(Pid, {reduce, RedFun, Initial}),

    % get resutls
    gen_fsm:sync_send_event(Pid, {result}).

advanced_job(Pid, NumWork, MapFun, RedFun, Initial, Data) ->
    job(Pid, NumWork, MapFun, RedFun, Initial, Data).

stop(Pid) ->
    gen_fsm:send_all_state_event(Pid, {stop}).

% get rid of warnings
init(_Args) ->
    {ok, alpha, []}.
handle_info(_Info, StateName, StateData) ->
    {next_state, StateName, StateData}.
handle_sync_event(_Event, _From, StateName, StateData) ->
    {reply, ok, StateName, StateData}.
code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.
handle_event(stop, _StateName, StateData) ->
    {stop, normal, StateData}.
terminate(normal, _StateName, _StateData) ->
    ok.
% mr:job(Pid,6,fun,lists:seq(1,10).
