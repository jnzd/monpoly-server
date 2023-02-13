open Predicate

type ltl_signature = { sup: var list;
                       cau: var list;
                       obs: var list }

type ltl_trace = (int * (var list)) list

type foltl_signature = { fsup: (var * int) list;
                         fcau: (var * int) list;
                         fobs: (var * int) list }

type foltl_trace = (int * ((var * int list) list)) list

type level = Cau | Sup | Obs

type predic = { name: var; arity: int; lvl: level }
