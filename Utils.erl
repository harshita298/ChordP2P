%%%-------------------------------------------------------------------
%%% @author ayushkumar
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Oct 2022 5:42 AM
%%%-------------------------------------------------------------------
-module('Utils').
-author("ayushkumar").
-define(NumBits, 256). %% using SHA256

%% API
-export([get_mapped_id_for_node/1, get_random_string/0]).

get_mapped_id_for_node(String) ->
  ShaList = integer_to_list(binary:decode_unsigned(crypto:hash(sha256, String)), 16),
  ShaInt = list_to_integer(ShaList, 16),
  Space = math:pow(2,256),
  ShaInt rem Space.


get_random_string() ->
  binary_to_list(base64:encode(crypto:strong_rand_bytes(16))).
