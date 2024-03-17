%%%-------------------------------------------------------------------
%%% @author ayushkumar
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Oct 2022 6:56 PM
%%%-------------------------------------------------------------------
-module(chordFixFingerManager).
-author("ayushkumar").

%% API
-export([start_link/2, stop/0, init/2, fix_finger_loop/0, fix_finger_for_node/0, set_next_node_to_fix/1, response_listener/0]).
-record(currBitNumber, {bit}).
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
  #currBitNumber{bit = 0},
  fix_finger_loop().

fix_finger_loop() ->
  fix_finger_for_node(),
  timer:sleep(10),
  fix_finger_loop().

fix_finger_for_node() ->
  if
    #currBitNumber.bit == 1 ->
      #currNodeBeingFixed.pid ! {giveImmediateSuccessor , self()},
      {SuccId, SuccPid} = response_listener(),
      #currNodeBeingFixed.pid ! {fixFinger, #currBitNumber.bit, SuccId, SuccPid};
    true ->
      #currNodeBeingFixed.pid ! {giveMthSuccessor, #currBitNumber.bit - 1, self()},
      {MM1SuccId, MM1SuccPid} = response_listener(),
      MM1SuccPid ! {giveMthSuccessor, #currBitNumber.bit - 1, self()},
      {SuccId, SuccPid} = response_listener(),
      #currNodeBeingFixed.pid ! {fixFinger, #currBitNumber.bit, SuccId, SuccPid}
  end,
  set_next_node_to_fix(#currNodeBeingFixed).


set_next_node_to_fix(#currNodeBeingFixed{
  id = CurrId,
  pid = CurrPid
} = CurrNodeBeingFixed) ->
  CurrPid ! {giveImmediateSuccessor , self()},
  {SuccId, SuccPid} = response_listener(),
  CurrNodeBeingFixed#currNodeBeingFixed{id = SuccId, pid = SuccPid},
  if
  SuccId == #firstNodeToFix.id -> #currBitNumber{bit = (#currBitNumber.bit + 1) rem ?NumBits}
end.


response_listener() ->
  receive
    {immediateSuccessor, SuccId, SuccPid} -> {SuccId, SuccPid};
    {mthSuccessor, SuccId, SuccId} -> {SuccId, SuccId}
  end.






