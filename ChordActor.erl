%%%-------------------------------------------------------------------
%%% @author ayushkumar
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Oct 2022 5:28 AM
%%%-------------------------------------------------------------------
-module('ChordActor').
-author("ayushkumar").

%% API
-export([start_link/3, stop/0, init/3, initialize_self_finger_table/3, listener_loop/0, find_successor/1, find_predecessor_from_ft/2, lookup_successor/1, contains/2, check_close/0, return_key_value/3, save_key/1]).
-record(nodestate, {id, keys, succId, preId, succPid, prePid, pid}).
-record(fingertable, {map}).
-record(constraints, {numBits, numRequestsLeft}).

start_link(NumBits, NumRequests, JoinerNodePid) ->
  Pid = spawn(?MODULE, init, [NumBits, NumRequests, JoinerNodePid]),
  register(?MODULE, Pid),
  Pid.

stop() ->
  self() ! terminate.

init(NumBits, NumRequests, JoinerNodePid) ->
  Id = 'Utils':get_mapped_id_for_node(self()),
  if JoinerNodePid == null ->
    NodeState = #nodestate{id = Id, keys = [], succId = Id, preId = Id, succId = self(), preId = self(), pid = self()};
    true ->
      JoinerNodePid ! {giveSuccessor, Id, self()},
      receive
        {successor, SuccId, SuccPid} -> {SuccId, SuccPid},
          NodeState = #nodestate{id = Id, keys = [], succId = SuccId, preId = Id, succId = SuccPid, preId = self(), pid = self()},
          SuccPid ! {notify, Id, self()}
      end
  end,
  FingerTableState = #fingertable{map = maps:new()},
  #constraints{numBits = NumBits, numRequestsLeft = NumRequests},
  initialize_self_finger_table(_id, NumBits, FingerTableState).

initialize_self_finger_table(_id, NumBits,
    #fingertable{map = Map} = FingerTable) when NumBits > 0 ->
  UpdatedMap = Map:put(NumBits, {_id, self()}),
  initialize_self_finger_table(_id, NumBits - 1, FingerTable#fingertable{map = UpdatedMap}),
  listener_loop().

listener_loop() ->
  receive
    {giveSuccessor, AskerId, AskerPid} -> AskerPid ! {successor, find_successor(AskerId)};
    {giveImmediateSuccessor, AskerPid} -> AskerPid ! {immediateSuccessor, #nodestate.succId, #nodestate.succPid};
    {giveImmediatePredecessor, AskerPid} -> AskerPid ! {immediatePredecessor, #nodestate.preId, #nodestate.prePid};
    {notify, NodeIdThatSetMeSucc, NodePIdThatSetMeSucc} -> #nodestate{id = #nodestate.id, keys = #nodestate.keys, succId = #nodestate.succId, preId = NodeIdThatSetMeSucc, succPid =  #nodestate.succPid, prePid = NodePIdThatSetMeSucc, pid = #nodestate.pid};
    {giveMthSuccessor, M, AskerPid} -> AskerPid ! {mthSuccessor, lookup_successor(M)};
    {fixFinger, M, Id, Pid} ->
      Map = #fingertable.map,
      maps:put(M, {Id, Pid}, Map);
    {updateSuccessor, SPreId, SPrePid} -> #nodestate{id = #nodestate.id, keys = #nodestate.keys, succId = SPreId, preId = #nodestate.preId, succPid = SPrePid, prePid = #nodestate.prePid, pid = #nodestate.pid};
    {updatePredecessor, UPreId, UPrePid} -> #nodestate{id = #nodestate.id, keys = #nodestate.keys, succId = #nodestate.succId, preId = UPreId, succPid = #nodestate.succPid, prePid = UPrePid, pid = #nodestate.pid};
    {saveKey, Key_id, Key_value} ->
      {SuccId, SuccPid} = find_successor(Key_id),
      if
      SuccId == #nodestate.id -> save_key(Key_value);
      true -> SuccPid ! {saveKey, Key_id, Key_value}
      end;
    {findKey, Key_id, Key_value, HopsTillNow, AskerPid} ->
      {SuccId, SuccPid} = find_successor(Key_id),
      if
        SuccId == #nodestate.id -> return_key_value(Key_value, AskerPid, HopsTillNow);
        true -> SuccPid ! {findKey, Key_id, Key_value, HopsTillNow, AskerPid}
      end,
    check_close();
    terminate ->
      io:format("listener stopped"), exit(normal)
  end,
  listener_loop().

find_successor(AskerId) ->
  {PreId, PrePid} = find_predecessor_from_ft(AskerId, lists:reverse(maps:values(#fingertable.map))),
  PrePid ! {giveImmediateSuccessor, self()},
  receive
    {immediateSuccessor, SuccId, SuccPid} -> {SuccId, SuccPid}
  end.

find_predecessor_from_ft(_, []) -> {#nodestate.id, #nodestate.pid};
find_predecessor_from_ft(AskerId, [A | FtTuples]) ->
  {Id, Pid} = A,
  if
    AskerId > Id -> {Id, Pid};
    true -> find_predecessor_from_ft(AskerId, FtTuples)
  end.

lookup_successor(M) ->
  {Id, Pid} = maps:get(M, #fingertable.map, {#nodestate.id, #nodestate.pid}),
  {Id, Pid}.

save_key(Key) ->
  Keys = lists:append(#nodestate.keys, [Key])
  #nodestate{id = #nodestate.id, keys = Keys, succId = #nodestate.succId, preId = #nodestate.preId, succPid = #nodestate.succPid, prePid = #nodestate.prePid, pid = #nodestate.pid}.

return_key_value(Key_value, AskerPid, HopsTillNow) ->
  Keys = #nodestate.keys,
  Isfound = contains(Key_value, Keys),
  if Isfound == true ->
    io:format("Found key =~p. Hops = ~p ~n", [Key_value, HopsTillNow + 1]),
    AskerPid ! {foundKey, Key_value, self(), HopsTillNow + 1};
    true ->
      io:format("Couldn't Find key =~p. Hops = ~p ~n", [Key_value, HopsTillNow + 1]),
      AskerPid ! {keyNotFound, null, self(), HopsTillNow + 1}
  end.

%finding lower bound
binary_search(NodeToAddInFingerTable, Low, High, Keys) when Low > High-> [Low].
binary_search(NodeToAddInFingerTable, Low, High, Keys) when Low < High->
  Mid=(Low+High)/2,
  if NodeToAddInFingerTable < Mid ->
    binarySearch(NodeToAddInFingerTable,Low,Mid,Keys);
    true->
      binarySearch(NodeToAddInFingerTable,Mid+1,High,Keys)
  end.

for(0,_,Keys) ->
  [].

for (N,0,Keys,NodesToAdd) ->
  Lower=binarySearch(Keys,0,N,Keys),
  Upper=binarySearch(Keys,0,N,Keys),
  Finger=0,
  if(Lower>=Keys)->
    Finger=Lower;
    true->Finger=Upper
  end,
  NodesToAdd:put(Keys,Finger),
  for(N-1,0,Keys).

get_finger_table(Id, M, I, Keys, Finger_table) ->
  %NodeToAddInFingerTable = Id + pow(2,I-1),
  NodesToAdd = {},
  N=length(Keys),
  for(N,0,Keys,NodesToAdd),

  %putting node in finger table
  [NodesToAdd].

check_close() ->
  #constraints{numBits =  #constraints.numBits, numRequestsLeft = #constraints.numRequestsLeft - 1},
  if #constraints.numRequestsLeft == 0 ->
    self() ! terminate
  end.

contains(_, []) -> false;
contains(Value, [H|T]) ->
  case H of
    Value -> true;
    _ -> contains(Value, T)
  end.




