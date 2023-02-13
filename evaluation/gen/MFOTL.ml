(*
 * This file is part of MONPOLY.
 *
 * Copyright (C) 2011 Nokia Corporation and/or its subsidiary(-ies).
 * Contact:  Nokia Corporation (Debmalya Biswas: debmalya.biswas@nokia.com)
 *
 * Copyright (C) 2012 ETH Zurich.
 * Contact:  ETH Zurich (Eugen Zalinescu: eugen.zalinescu@inf.ethz.ch)
 *
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation, version 2.1 of the
 * License.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library. If not, see
 * http://www.gnu.org/licenses/lgpl-2.1.html.
 *
 * As a special exception to the GNU Lesser General Public License,
 * you may link, statically or dynamically, a "work that uses the
 * Library" with a publicly distributed version of the Library to
 * produce an executable file containing portions of the Library, and
 * distribute that executable file under terms of your choice, without
 * any of the additional requirements listed in clause 6 of the GNU
 * Lesser General Public License. By "a publicly distributed version
 * of the Library", we mean either the unmodified Library as
 * distributed by Nokia, or a modified version of the Library that is
 * distributed under the conditions defined in clause 3 of the GNU
 * Lesser General Public License. This exception does not however
 * invalidate any other reasons why the executable file might be
 * covered by the GNU Lesser General Public License.
 *)



open Unix
open Predicate

type timestamp = float
type tsdiff = timestamp
type bound = OBnd of tsdiff | CBnd of tsdiff | Inf
type interval = bound * bound

type agg_op = Cnt | Min | Max | Sum | Avg | Med
type enf_flag = Left | Right | Neither

let agg_default_value op ty =
  match op, ty with
  | Min, TFloat -> Float infinity
  | Max, TFloat -> Float neg_infinity
  | _, TFloat -> Float 0.
  | _, TInt -> Int 0
  | _, TStr -> Str ""


type formula =
  | Equal of (term * term)
  | Less of (term * term)
  | LessEq of (term * term)
  | Pred of predicate
  | Neg of formula
  | And of (formula * formula * enf_flag)
  | Or of (formula * formula)
  | Implies of (formula * formula)
  | Equiv of (formula * formula)
  | Exists of (var list * formula)
  | ForAll of (var list * formula)
  | Aggreg of (var * agg_op * var * var list * formula)
  | Prev of (interval * formula)
  | Next of (interval * formula)
  | Eventually of (interval * formula)
  | Once of (interval * formula)
  | Always of (interval * formula)
  | PastAlways of (interval * formula)
  | Since of (interval * formula * formula)
  | Until of (interval * formula * formula)

let unixts = ref false

let ts_of_string err_place str =
  try
    float_of_string str
  with Failure _ ->
    let msg = Printf.sprintf "[%s, MFOTL.ts_of_string] Cannot convert %s into a timestamp" err_place str in
    failwith msg

let ts_of_cst c =
  match c with
  | Int _ -> failwith "[MFOTL.ts_of_cst] conversion not possible"
  | Str s -> float_of_string s
  | Float f -> f
let cst_of_ts t = Str (string_of_float t)
let tsdiff_of_cst = ts_of_cst
let cst_of_tsdiff = cst_of_ts

let ts_plus t1 t2 = t1 +. t2
let ts_minus t1 t2 = t1 -. t2
let ts_invalid = -1.
let ts_null = 0.
let ts_max = max_float


let _ts_of_string2 err_place str =
  try
    int_of_string str
  with Failure _ ->
    (* a way of avoiding the problem of too big integers *)
    let lens = String.length str in
    let lenm = String.length (string_of_int max_int) in
    let str =
      if lens >= lenm then
        let d = lens-lenm+1 in
        String.sub str d (lens-d)
      else
        str
    in
    try
      int_of_string str
    with Failure _ ->
      let msg = Printf.sprintf "[%s, MFOTL.ts_of_string] Cannot convert %s into a timestamp" err_place str in
      failwith msg



(* TODO: these function names are not intuitive, because one usually
   does not think in terms of the interval I labelling a temporal
   operator, but instead, in terms of the time window that it defines.
   So update these, with something like, left_of_timewindow,
   right_of_timewindow, in_timewindow.
*)

(* Tests that [v] is in the extension to the right of [intv] *)
let in_right_ext v intv =
  match intv with
  | CBnd b, _ -> v >= b
  | OBnd b, _ -> v > b
  | Inf, _ -> false

(* Tests that [v] is in the extension to the left of [intv] *)
let in_left_ext v intv =
  match intv with
  | _, CBnd b -> v <= b
  | _, OBnd b -> v < b
  | _, Inf -> true

let in_interval v intv =
  in_right_ext v intv && in_left_ext v intv

let is_infinite_interval (_, b) = (b = Inf)

let init_interval (_, b) = (CBnd 0., b)



let direct_subformulas = function
  | Equal _
  | Less _
  | LessEq _
  | Pred _ -> []
  | Neg f -> [f]
  | And (f1,f2,_) -> [f1;f2]
  | Or (f1,f2) -> [f1;f2]
  | Implies (f1,f2) -> [f1;f2]
  | Equiv (f1,f2) -> [f1;f2]
  | Exists (_v,f) -> [f]
  | ForAll (_v,f) -> [f]
  | Aggreg (_,_,_,_,f) -> [f]
  | Prev (_intv,f) -> [f]
  | Next (_intv,f) -> [f]
  | Eventually (_intv,f) -> [f]
  | Once (_intv,f) -> [f]
  | Always (_intv,f) -> [f]
  | PastAlways (_intv,f) -> [f]
  | Since (_intv,f1,f2) -> [f1;f2]
  | Until (_intv,f1,f2) -> [f1;f2]


let is_temporal = function
  | Prev _
  | Next _
  | Since _
  | Until _
  | Eventually _
  | Once _
  | Always _
  | PastAlways _
  | _ -> false


let rec free_vars = function
  | Equal (t1,t2)
  | Less (t1,t2)
  | LessEq (t1,t2) ->
    Misc.union (Predicate.tvars t1) (Predicate.tvars t2)
  | Pred p -> Predicate.pvars p
  | Neg f -> free_vars f
  | And (f1,f2,_) -> Misc.union (free_vars f1) (free_vars f2)
  | Or (f1,f2) -> Misc.union (free_vars f1) (free_vars f2)
  | Implies (f1,f2) -> Misc.union (free_vars f1) (free_vars f2)
  | Equiv (f1,f2) -> Misc.union (free_vars f1) (free_vars f2)
  | Exists (vl,f) -> List.filter (fun x -> not (List.mem x vl)) (free_vars f)
  | ForAll (vl,f) -> List.filter (fun x -> not (List.mem x vl)) (free_vars f)
  | Aggreg (y,_op,_x,glist,_f) -> y :: glist
  | Prev (_intv,f) -> free_vars f
  | Next (_intv,f) -> free_vars f
  | Eventually (_intv,f) -> free_vars f
  | Once (_intv,f) -> free_vars f
  | Always (_intv,f) -> free_vars f
  | PastAlways (_intv,f) -> free_vars f
  | Since (_intv,f1,f2) -> Misc.union (free_vars f2) (free_vars f1)
  | Until (_intv,f1,f2) -> Misc.union (free_vars f2) (free_vars f1)


let avoid_capture v fv m =
  let m_range = List.flatten (List.map (fun (_,t) -> Predicate.tvars t) m) in
  let capturing = List.filter (fun x -> List.mem x m_range) v in
  let fresh = Predicate.fresh_var_mapping (Misc.union fv m_range) capturing in
  let m' = List.filter (fun (x,_) -> not (List.mem x v)) m in
  (fresh, m' @ Predicate.mk_subst fresh)

let subst_vars m = List.map (fun x -> try List.assoc x m with Not_found -> x)

let rec substitute_vars m = function
  | Equal (t1, t2) -> Equal (Predicate.substitute_vars m t1, Predicate.substitute_vars m t2)
  | Less  (t1, t2) -> Less (Predicate.substitute_vars m t1, Predicate.substitute_vars m t2)
  | LessEq (t1, t2) -> LessEq (Predicate.substitute_vars m t1, Predicate.substitute_vars m t2)
  | Pred (p) ->
    let (n, _a, ts) = Predicate.get_info p in
    Pred (Predicate.make_predicate (n, List.map (Predicate.substitute_vars m) ts))
  | Neg f -> Neg (substitute_vars m f)
  | Exists (v, f) ->
      let fresh, m' = avoid_capture v (free_vars f) m in
      Exists (subst_vars fresh v, substitute_vars m' f)
  | ForAll (v, f) ->
      let fresh, m' = avoid_capture v (free_vars f) m in
      ForAll (subst_vars fresh v, substitute_vars m' f)
  | Aggreg (y, op, x, g, f) ->
      let fv = free_vars f in
      let bound = Misc.diff fv g in
      let fresh, m' = avoid_capture bound fv m in
      let subst m v = try List.assoc v m with Not_found -> Var v in
      let unvar = function
        | Var x -> x
        | _ -> failwith "[MFOTL.substitute_vars] cannot substitute aggregation variable with term"
      in
      let y = unvar (subst m y) in
      let x = try List.assoc x fresh with Not_found -> x in
      let g = List.map (fun x -> unvar (subst m x)) g in
      Aggreg (y, op, x, g, substitute_vars m' f)
  | Prev (i, f) -> Prev (i, substitute_vars m f)
  | Next (i, f) -> Next (i, substitute_vars m f)
  | Once (i, f) -> Once (i, substitute_vars m f)
  | PastAlways (i, f) -> PastAlways (i, substitute_vars m f)
  | Eventually (i, f) -> Eventually (i, substitute_vars m f)
  | Always (i, f) -> Always (i, substitute_vars m f)
  | And (f1, f2, f) -> And (substitute_vars m f1, substitute_vars m f2, f)
  | Or (f1, f2) -> Or (substitute_vars m f1, substitute_vars m f2)
  | Implies (f1, f2) -> Implies (substitute_vars m f1, substitute_vars m f2)
  | Equiv (f1, f2) ->  Equiv (substitute_vars m f1, substitute_vars m f2)
  | Since (i, f1, f2) -> Since (i, substitute_vars m f1, substitute_vars m f2)
  | Until (i, f1, f2) -> Until (i, substitute_vars m f1, substitute_vars m f2)


let string_of_ts ts =
  if !unixts then
    let tm = Unix.gmtime ts in
    Printf.sprintf "%d-%d-%d %d:%d:%d"
      tm.tm_year tm.tm_mon tm.tm_mday
      tm.tm_hour tm.tm_min tm.tm_sec
  else
    if ts = ts_max then
      "MaxTS"
    else
      Printf.sprintf "%F" ts

let print_ts ts =
  print_string (string_of_ts ts)

let string_of_interval (a,b) =
  (match a with
    | OBnd a -> Printf.sprintf "(%.0f," a
    | CBnd a -> Printf.sprintf "[%.0f," a
    | Inf -> Printf.sprintf "(*,")
  ^
  (match b with
  | OBnd b -> Printf.sprintf "%.0f)" b
  | CBnd b -> Printf.sprintf "%.0f]" b
  | Inf -> Printf.sprintf "*)")

let print_interval (a,b) =
  Printf.printf "%s" (string_of_interval (a,b))

let string_of_agg_op = function
  | Cnt -> "CNT"
  | Min -> "MIN"
  | Max -> "MAX"
  | Sum -> "SUM"
  | Avg -> "AVG"
  | Med -> "MED"

(* we always put parentheses for binary operators like "(f1 AND f2)", and around unary
ones only if they occur on the left-hand side of a binary operator: like "((NOT f1) AND f2)"*)
let string_of_formula str g =
  let rec string_f_rec top par h =
    (match h with
      | Equal (t1,t2) ->
        Predicate.string_of_term t1 ^ " = " ^ Predicate.string_of_term t2
      | Less (t1,t2) ->
        Predicate.string_of_term t1 ^ " < " ^ Predicate.string_of_term t2
      | LessEq (t1,t2) ->
        Predicate.string_of_term t1 ^ " <= " ^ Predicate.string_of_term t2
      | Pred p -> Predicate.string_of_predicate p
      | _ ->
        (if par && not top then "(" else "")
        ^ 
        (match h with
        | Neg f ->
          "NOT " ^ string_f_rec false false f

        | Exists (vl,f) ->
          "EXISTS " 
          ^ 
          (Misc.string_of_list_ext "" "" ", " Predicate.string_of_term (List.map (fun v -> Var v) vl))
          ^
          ". "
          ^
          (string_f_rec false false f)

        | ForAll (vl,f) ->
          "FORALL "
          ^
          (Misc.string_of_list_ext "" "" ", " Predicate.string_of_term (List.map (fun v -> Var v) vl))
          ^
          ". "
          ^
          (string_f_rec false false f)

        | Aggreg (y,op,x,glist,f) ->
          (Predicate.string_of_term (Var y))
          ^
          " <- "
          ^
          (string_of_agg_op op)
          ^
          " "
          ^
          (Predicate.string_of_term (Var x))
          ^
          (if glist <> [] then
              "; "
              ^
              (Misc.string_of_list_ext "" "" "," (fun z -> Predicate.string_of_term (Var z)) glist)
          else "")
          ^
          " "
          ^
          (string_f_rec false false f)

        | Prev (intv,f) ->
          "PREVIOUS"
          ^
          (string_of_interval intv)
          ^
          " "
          ^
          (string_f_rec false false f)

        | Next (intv,f) ->
          "NEXT"
          ^
          (string_of_interval intv)
          ^
          " "
          ^
          (string_f_rec false false f)

        | Eventually (intv,f) ->
          "EVENTUALLY"
          ^
          (string_of_interval intv)
          ^
          " "
          ^
          (string_f_rec false false f)

        | Once (intv,f) ->
          "ONCE"
          ^
          (string_of_interval intv)
          ^
          " "
          ^
          (string_f_rec false false f)

        | Always (intv,f) ->
          "ALWAYS"
          ^
          (string_of_interval intv)
          ^
          " "
          ^
          (string_f_rec false false f)

        | PastAlways (intv,f) ->
          "PAST_ALWAYS"
          ^
          (string_of_interval intv)
          ^
          " "
          ^
          (string_f_rec false false f)

        | _ ->
          (if not par && not top then "(" else "")
          ^
          (match h with
            | And (f1,f2,_) ->
              (string_f_rec false true f1)
              ^
              " AND "
              ^
              (string_f_rec false true f2)
            | Or (f1,f2) ->
              (string_f_rec false true f1)
              ^
              " OR "
              ^
              (string_f_rec false false f2)

            | Implies (f1,f2) ->
              (string_f_rec false true f1)
              ^
              " IMPLIES "
              ^
              (string_f_rec false false f2)

            | Equiv (f1,f2) ->
              (string_f_rec false true f1)
              ^
              " EQUIV "
              ^
              (string_f_rec false false f2)

            | Since (intv,f1,f2) ->
              (string_f_rec false true f1)
              ^ 
              " SINCE"
              ^
              (string_of_interval intv)
              ^
              " "
              ^
              (string_f_rec false false f2)

            | Until (intv,f1,f2) ->
              (string_f_rec false true f1)
              ^
              " UNTIL"
              ^
              (string_of_interval intv)
              ^
              " "
              ^
              (string_f_rec false false f2)
            | _ -> failwith "[print_formula] impossible"
          )
          ^
          (if not par && not top then ")" else "")
        ) 
        ^
        (if par && not top then ")" else "")
    )
  in
  str ^ (string_f_rec true false g)

  (* Fully parenthesize an MFOTL formula *)
  (* TODO: Seems unused? *)
  (*let string_of_parenthesized_formula str g =
    let rec string_f_rec top par h =
      (match h with
        | Equal (t1,t2) ->
          "(" ^ (Predicate.string_of_term t1) ^ " = " ^ (Predicate.string_of_term t2) ^ ")"
        | Less (t1,t2) ->
          "(" ^ (Predicate.string_of_term t1) ^ " < " ^ (Predicate.string_of_term t2) ^ ")"
        | LessEq (t1,t2) ->
          "(" ^ (Predicate.string_of_term t1) ^ " <= " ^ (Predicate.string_of_term t2) ^ ")"
        | Pred p -> Predicate.string_of_predicate p
        | _ ->
           
          (match h with
          | Neg f ->
            "(" ^
            "NOT " ^ (string_f_rec false false f)
            ^ ")"
  
          | Exists (vl,f) ->
            "(" ^
            "EXISTS " 
            ^ 
            (Misc.string_of_list_ext "" "" ", " Predicate.string_of_term (List.map (fun v -> Var v) vl) )
            ^
            ". "
            ^
            (string_f_rec false false f)
            ^ ")"
  
          | ForAll (vl,f) ->
            "(" ^
            "FORALL "
            ^
            (Misc.string_of_list_ext "" "" ", " Predicate.string_of_term (List.map (fun v -> Var v) vl))
            ^
            ". "
            ^
            (string_f_rec false false f)
            ^ ")"
  
          | Aggreg (y,op,x,glist,f) ->
            "(" ^
            (Predicate.string_of_term (Var y))
            ^
            " <- "
            ^
            (string_of_agg_op op)
            ^
            " "
            ^
            (Predicate.string_of_term (Var x))
            ^
            (if glist <> [] then
                "; "
                ^
                (Misc.string_of_list_ext "" "" "," (fun z -> Predicate.string_of_term (Var z)) glist)
            else "")
            ^
            " "
            ^
            (string_f_rec false false f)
            ^ ")"
  
          | Prev (intv,f) ->
            "(" ^
            "PREVIOUS"
            ^
            (string_of_interval intv)
            ^
            " "
            ^
            (string_f_rec false false f)
            ^ ")"
  
          | Next (intv,f) ->
            "(" ^
            "NEXT"
            ^
            (string_of_interval intv)
            ^
            " "
            ^
            (string_f_rec false false f)
            ^ ")"
  
          | Eventually (intv,f) ->
            "(" ^
            "EVENTUALLY"
            ^
            (string_of_interval intv)
            ^
            " "
            ^
            (string_f_rec false false f)
            ^ ")"
  
          | Once (intv,f) ->
            "(" ^
            "ONCE"
            ^
            (string_of_interval intv)
            ^
            " "
            ^
            (string_f_rec false false f)
            ^ ")"
  
          | Always (intv,f) ->
            "(" ^
            "ALWAYS"
            ^
            (string_of_interval intv)
            ^
            " "
            ^
            (string_f_rec false false f)
            ^ ")"
  
          | PastAlways (intv,f) ->
            "(" ^
            "PAST_ALWAYS"
            ^
            (string_of_interval intv)
            ^
            " "
            ^
            (string_f_rec false false f)
            ^ ")"
            
          | _ ->
 
            (match h with
              | And (f1,f2) ->
                "(" ^
                (string_f_rec false true f1)
                ^
                " AND " 
                ^
                (string_f_rec false true f2)
                ^ ")"
  
              | Or (f1,f2) ->
                "(" ^
                (string_f_rec false true f1)
                ^
                " OR "
                ^
                (string_f_rec false false f2)
                ^ ")"
  
              | Implies (f1,f2) ->
                "(" ^
                (string_f_rec false true f1)
                ^
                " IMPLIES "
                ^
                (string_f_rec false false f2)
                ^ ")"
  
              | Equiv (f1,f2) ->
                "(" ^
                (string_f_rec false true f1)
                ^
                " EQUIV "
                ^
                (string_f_rec false false f2)
                ^ ")"
  
              | Since (intv,f1,f2) ->
                "(" ^
                (string_f_rec false true f1)
                ^ 
                " SINCE"
                ^
                (string_of_interval intv)
                ^
                " "
                ^
                (string_f_rec false false f2)
                ^ ")"
  
              | Until (intv,f1,f2) ->
                "(" ^
                (string_f_rec false true f1)
                ^
                " UNTIL"
                ^
                (string_of_interval intv)
                ^
                " "
                ^
                (string_f_rec false false f2)
                ^ ")"
              | _ -> failwith "[print_formula] impossible"
            )
            
          ) 
      )
      in
    str ^ (string_f_rec true false g)
*)

let print_formula str f =
  print_string (string_of_formula str f)

let printnl_formula str f =
  print_formula str f;
  print_newline()

let float_of_ts ts = ts
let ts_of_float ts = ts
