-module(master).
-behaviour(gen_fsm).

-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).
-export([wait/3,init/3,map/3,reduce/3]).

-record(state, {workers :: queue()}).

init(master) ->
    Queue = generate_workers_pool(queue:new(), 20),
    % io:format("QUEUE:: - ~p ~n", [queue:to_list(Queue)]),
    {ok, wait,  #state{workers = Queue}}.

generate_workers_pool(Q, Nr) ->
    case Nr of
        0 -> Q;
        _ -> {ok, Worker} = gen_fsm:start_link(worker,worker, []),
             Queue = queue:in(Worker, Q),
             generate_workers_pool(Queue, Nr-1)
    end.


wait({init, NumWork, Data}, _From, S=#state{}) ->
    % io:format("Job received! ~n"),
    Length = length(Data),
    start_mappers(S#state.workers, NumWork -1, Data, Length div (NumWork-1)),
    {reply, ok, init, S};
wait(Event, _From, StateData) ->
    unexpected(Event, StateData),
    % io:format("DATA: ~p ~n", [StateData]),
    {reply, {error, unknown_op}, wait, StateData}.


% starting workers (check if workers are in wait?)
start_mappers(Workers, Nr, Data, Skip) ->
    case Nr of
        % last worker should get all remaining data
        1 -> {{_, Current}, _} = queue:out(Workers),
            %  io:format("Last_mapper ~p : ~p ~n", [Data, Current]),
             gen_fsm:send_event(Current, {mapper,  Data});
            %  start_mappers(Q, 0, Data, Skip);

        _ -> {{_, Current}, Q} = queue:out(Workers),
             {SplittedData, Rest} = lists:split(Skip, Data),
            %  io:format("Worker ~p ------- ~p ~n", [SplittedData, Current]),
             gen_fsm:send_event(Current, {mapper,  SplittedData}),
             start_mappers(Q, Nr-1, Rest, Skip)
    end.

%init -> map
init({map, NumWork, MapFun}, _From, S=#state{}) ->
    % io:format("Init received ~p starting mapping ~n", [From]),
    % io:format("Workers list: ~p ~n", [queue:to_list(S#state.workers)]),
    start_mapping(NumWork-1, S#state.workers ,MapFun),
    % {{_, Wid}, Q} = queue:out(S#state.workers),
    % gen_fsm:send_all_state_event(S#state.workers, {wait, MapFun}),
    % gen_fsm:send_all_state_event(worker, {wait, MapFun}),

    {reply, ok, map, S}.

start_mapping(Nr,Queue, MapFun) ->
    case Nr of
        0 -> nothing;
        _ ->  {{_, Wid}, Q} = queue:out(Queue),
              gen_fsm:send_event(Wid, {map, MapFun}),
              start_mapping(Nr-1, Q, MapFun)
    end.

map({reduce, RedFun, Initial}, _From, S=#state{}) ->
    % io:format("Starting reducing ~n"),

    % get data from the mappers
    Intermediate = get_data_from_mappers(S#state.workers, []),

    Med = lists:append(Intermediate),
    % io:format("Intermediate data: ~p ~n", [Med]),

    send_data_to_reducers(Med, RedFun, Initial, S#state.workers),
    {reply, ok, reduce, S}.

% iterate through all workers(dumb yeah) should stop iterating(todo) when nothing found
% shouldnt stack all data in a list(should pass it to reducers instead)
get_data_from_mappers(Queue, Data) ->
    case queue:out(Queue) of
        {empty, _} -> Data;
        {{_, Wid}, Q} ->
            % Leftovers = gen_fsm:sync_send_event(Wid, {reduce}),
            % io:format("Wid: ~p ~n", [Wid]),

            Leftovers = gen_fsm:sync_send_event(Wid, {reduce}),

            NewData = lists:append(Data, Leftovers),
            get_data_from_mappers(Q, NewData)
    end.


send_data_to_reducers(Data, RedFun, Initial, Queue) ->
    case queue:out(Queue) of
        {empty, _} -> nothing;
        {{_, Wid}, _} ->
            gen_fsm:sync_send_event(Wid, {reduce, RedFun, Initial, Data})
            % io:format("Leftovers: ~p ~n", [Leftovers]),
            % lists:append(Data, Leftovers),
            % io:format("Intermediate data: ~p ~n", [Data]),
            % get_data_from_mappers(Q, lists:append(Leftovers, Data))
    end.

reduce({result}, _From, S=#state{}) ->
    {{_, Wid}, _} = queue:out(S#state.workers),
    Result = gen_fsm:sync_send_event(Wid, {result}),
    % io:format("RESULT: ~p ~n", [Result]),

    {reply, {ok, Result}, wait, S}.

handle_event({stop}, _StateName, S=#state{}) ->
    % io:format("PID?: ~p ~n", [self()]),
    % {reply, ok, wait, StateData}.
    %stop workers

    stop_all_workers(S#state.workers),
    {stop, normal, []}.

stop_all_workers(Queue) ->
    case queue:out(Queue) of
        {empty, _} -> nothing;
        {{_, Wid}, Q} ->
            gen_fsm:send_all_state_event(Wid, {stop}),
            stop_all_workers(Q)
    end.

%% Unexpected allows to log unexpected messages
unexpected(Msg, State) ->
   io:format("~p received unknown event ~p while in state ~p~n",
   [self(), Msg, State]).

terminate(normal, _StateName, _StateData) ->
   ok.

handle_info(_Info, StateName, StateData) ->
   {next_state, StateName, StateData}.
handle_sync_event(_Event, _From, StateName, StateData) ->
   {reply, ok, StateName, StateData}.
code_change(_OldVsn, StateName, StateData, _Extra) ->
   {ok, StateName, StateData}.
