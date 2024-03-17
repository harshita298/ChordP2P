%%%-------------------------------------------------------------------
%%% @author ayushkumar
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Oct 2022 5:35 AM
%%%-------------------------------------------------------------------
-module('ChordServer').
-author("ayushkumar").
-define(NumBits, 256). %% using SHA256
-record(joinerNode, {id, pid}).
-record(keys, {keys}).
-record(avgHops, {hops, searches}).

%% API
-export([start_link/2, stop/0, init/2, spawn_all_actors/3, set_keys/1, find_keys_main/0, find_key/1]).

start_link(NumNodes, NumRequests) ->
  Pid = spawn(?MODULE, init, [NumNodes, NumRequests]),
  register(?MODULE, Pid).

stop() ->
  self() ! terminate.
  chordStabilizer:stop(),
  chordFixFingerManager:stop().

init(NumNodes, NumRequests) ->
  _id = 'Utils':get_mapped_id_for_node(self()),
  JoinerNodePid = 'ChordActor':start_link(?NumBits, NumRequests, null),
  JoinerNode = #joinerNode{id = 'Utils':get_mapped_id_for_node(JoinerNodePid), pid = JoinerNodePid},
  chordStabilizer:start_link(#joinerNode.id, #joinerNode.pid),
  chordFixFingerManager:start_link(#joinerNode.id, #joinerNode.pid),
  spawn_all_actors(NumNodes - 1, NumRequests, JoinerNodePid),
  listener_loop(),
  set_keys(10000),
  find_keys_main(),
  io:format("Average hops for a message = ~p ~n", [#avgHops.hops/#avgHops.searches]).



spawn_all_actors(NumNodes, NumRequests, JoinerNodePid) when NumNodes > 0 ->
  'ChordActor':start_link(?NumBits, NumRequests, JoinerNodePid),
  spawn_all_actors(NumNodes - 1, NumRequests, JoinerNodePid).

set_keys(NumKeys) when NumKeys > 0 ->
  RandomString = 'Utils':get_random_string(),
  Keys = lists:append(#keys.keys, [RandomString]),
  #keys{keys = Keys},
  Hashed = 'Utils':get_mapped_id_for_node(RandomString)
  #joinerNode.pid ! {saveKey, Hashed, RandomString}.

find_keys_main() ->
  find_key(#keys.keys).

find_key([]) -> ok;
find_key([H|T]) ->
  Hashed = 'Utils':get_mapped_id_for_node(H)
  #joinerNode.pid ! {saveKey, Hashed, H},
  find_key([T]).

listener_loop() ->
  receive
    {foundKey, Key_value, FinderPid, HopsTillNow} -> #avgHops{hops = #avgHops.hops + HopsTillNow, searches = #avgHops.searches + 1};
    terminate ->
      io:format("server listener stopped"), exit(normal)
  end,
  listener_loop().



