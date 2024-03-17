%%%-------------------------------------------------------------------
%%% @author ayushkumar
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Oct 2022 6:55 PM
%%%-------------------------------------------------------------------
-module(chordStabilizer).
-author("ayushkumar").

%% API
-export([start_link/2, stop/0, init/2, stabilize_loop/0, stabilize_node/0, set_next_node_to_fix/1, response_listener/0]).
-record(currNodeBeingFixed, {id, pid}).
-record(firstNodeToFix, {id, pid}).
-define(NumBits, 256).

start_link(JoinerNodeId, JoinerNodePid) ->
  Pid = spawn(?MODULE, init, [JoinerNodeId, JoinerNodePid]),
  register(?MODULE, Pid),
  Pid.

stop() ->
  exit(normal).

init(JoinerNodeId, JoinerNodePid) ->
  #currNodeBeingFixed{id = JoinerNodeId, pid = JoinerNodePid},
  #firstNodeToFix{id = JoinerNodeId, pid = JoinerNodePid},
  stabilize_loop().

stabilize_loop() ->
  stabilize_node(),
  timer:sleep(10),
  stabilize_loop().

stabilize_node() ->
  #currNodeBeingFixed.pid ! {giveImmediateSuccessor , self()},
  {SuccId, SuccPid} = response_listener(),
  SuccPid ! {giveImmediatePredecessor, self()},
  {SPreId, SPrePid} = response_listener(),

  if
    SPreId == #currNodeBeingFixed.pid -> ok;
    true ->
      #currNodeBeingFixed.pid ! {updateSuccessor, SPreId, SPrePid},
      SPrePid ! {updatePredecessor, #currNodeBeingFixed.id, #currNodeBeingFixed.pid}
  end,
  set_next_node_to_fix(#currNodeBeingFixed).


set_next_node_to_fix(#currNodeBeingFixed{
  id = CurrId,
  pid = CurrPid
} = CurrNodeBeingFixed) ->
  CurrPid ! {giveImmediateSuccessor , self()},
  {SuccId, SuccPid} = response_listener(),
  CurrNodeBeingFixed#currNodeBeingFixed{id = SuccId, pid = SuccPid}.

response_listener() ->
  receive
    {immediateSuccessor, SuccId, SuccPid} -> {SuccId, SuccPid};
    {mthSuccessor, SuccId, SuccId} -> {SuccId, SuccId};
    {immediatePredecessor, PreId, PrePid} -> {PreId, PrePid}
  end.

