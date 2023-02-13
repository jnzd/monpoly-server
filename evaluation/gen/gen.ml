
open Owl
open MFOTL
open Predicate

open Gen_types

(* Conversion *)

let ltl_of_mfotl_signature sign =
  { cau = List.map fst sign.fcau;
    sup = List.map fst sign.fsup;
    obs = List.map fst sign.fobs }

(* Generation *)

let choose l =
  List.nth l (Random.int (List.length l))

let rec apply l =
  try (choose l) ()
  with _ -> apply l

let random_ltl_predicate vars () =
  Pred (make_predicate (choose vars, []))

let rec random_ltl sign depth =
  let r = random_ltl sign in
  match depth with
    0 -> (let f = apply [random_ltl_predicate sign.sup;
                         random_ltl_predicate sign.cau;
                         random_ltl_predicate sign.obs] in
          if (Random.int 2) = 0 then f else Neg f)
  | i when i > 0 -> apply [(fun _ -> Or (r (depth-1), r (depth-1)));
                           (fun _ -> And (r (depth-1), r (depth-1), Neither));
                           (fun _ -> Neg (r (depth-1)));
                           (fun _ -> Prev ((CBnd 0., Inf), r (depth-1)));
                           (fun _ -> Once ((CBnd 0., Inf), r (depth-1)));
                           (fun _ -> Since ((CBnd 0., Inf), r (depth-1), r (depth-1)))]
  | _ -> failwith "random_ltl"

let rec random_enf_ltl sign depth =
  let r = random_enf_ltl sign in
  match depth with
    0 -> (if Random.int 1 = 0
          then random_ltl_predicate sign.sup ()
          else Neg (random_ltl_predicate sign.cau ()))
  | i when i > 0 -> apply [(fun _ -> Or (r (depth-1), r (depth-1)));
                           (fun _ -> And (r (depth-1), random_ltl sign (depth-1), Left))]
  | _ -> failwith "random_enf_ltl"

let random_ltl_trace sign n binsize length =
  let rec aux_bin = function
      0 -> []
    | i when i > 0 -> (choose (sign.sup @ sign.cau @ sign.obs)
                       :: aux_bin (i-1))
    | _ -> failwith "random_ltl_trace 1" in
  let rec aux ts = function
      0 -> []
    | i when i > 0 -> (let ts' = ts + 1 in
                       (ts', aux_bin (Stats.binomial_rvs ~p:binsize ~n))
                       :: (aux ts' (i-1)))
    | _ -> failwith "random_ltl_trace 2"
  in aux 0 length

let rec repeat_until f c () = 
   let r = f () in
   if c r then r else repeat_until f c ()

let random_mfotl_predicate preds fv () =
  let rec aux_arg = function
      0 -> []
    | i when i > 0 -> (Var ("x" ^ (string_of_int (Random.int fv)))) :: (aux_arg (i-1))
    | _ -> failwith "random_mfotl_predicate" in
  let pred = choose preds in
   Pred (make_predicate (fst pred, aux_arg (snd pred)))

let rec random_mon_mfotl_ sign depth mbound fv =
  let r = random_mon_mfotl_ sign (depth-1) mbound in
  match depth with
    0 -> apply [random_mfotl_predicate sign.fsup fv;
                random_mfotl_predicate sign.fcau fv;
                random_mfotl_predicate sign.fobs fv]
  | i when i > 0 -> apply [(fun _ -> (let (f1, f2) =
                                        repeat_until (fun _ -> (r fv, r fv))
                                          (fun (f1, f2) -> ((Misc.subset (free_vars f1) (free_vars f2))
                                                            && (Misc.subset (free_vars f2) (free_vars f1)))) ()
                                      in Or (f1, f2)));
                           (fun _ -> And (r  fv, r fv, Neither));
                           (fun _ -> (let (f1, f2) =
                                        repeat_until (fun _ -> (r fv, r fv))
                                          (fun (f1, f2) -> ((Misc.subset (free_vars f2) (free_vars f1)))) ()
                                      in And (f1, Neg f2, Neither)));
                           (fun _ -> let lb = Random.float mbound in
                                     Prev ((CBnd lb, CBnd (lb +. Random.float mbound)), r fv));
                           (fun _ -> let lb = Random.float mbound in
                                     Once ((CBnd lb, CBnd (lb +. Random.float mbound)), r fv));
                           (fun _ -> (let (f1, f2) =
                                        repeat_until (fun _ -> (r fv, r fv))
                                          (fun (f1, f2) -> ((Misc.subset (free_vars f1) (free_vars f2)))) ()
                                      in let lb = Random.float mbound in
                                         Since ((CBnd lb, CBnd (lb +. Random.float mbound)),
                                                (if (Random.int 2) = 0 then f1 else Neg f1), f2)));
                           (fun _ -> let new_var = "x" ^ (string_of_int fv) in
                                     let f = repeat_until (fun _ -> r (fv + 1)) (fun f -> List.mem new_var (free_vars f)) () in
                                     Exists ([new_var], f))]
  | _ -> failwith "random_mon_mfotl"

let rec random_enf_mon_mfotl_ sign depth mbound fv =
  let r = random_enf_mon_mfotl_ sign (depth-1) mbound in
  match depth with
    0 -> (if Random.int 1 = 0
          then random_mfotl_predicate sign.fsup fv ()
          else Neg (random_mfotl_predicate sign.fcau fv ()))
  | i when i > 0 -> apply [(fun _ -> (let (f1, f2) =
                                        repeat_until (fun _ -> (r fv, r fv))
                                          (fun (f1, f2) -> ((Misc.subset (free_vars f1) (free_vars f2))
                                                            && (Misc.subset (free_vars f2) (free_vars f1)))) ()
                                      in Or (f1, f2)));
                           (fun _ -> And (r fv, random_mon_mfotl_ sign (depth-1) mbound fv, Left));
                           (fun _ -> (let (f1, f2) =
                                        repeat_until (fun _ -> (r fv, random_mon_mfotl_ sign (depth-1) mbound fv))
                                          (fun (f1, f2) -> ((Misc.subset (free_vars f2) (free_vars f1)))) ()
                                      in
                                      And (f1, Neg f2, Left)));
                           (fun _ -> let new_var = "x" ^ (string_of_int fv) in
                                     let f = repeat_until (fun _ -> r (fv + 1)) (fun f -> List.mem new_var (free_vars f)) () in
                                     Exists ([new_var], f))]
  | _ -> failwith "random_enf_mon_mfotl"

let random_enf_mon_mfotl sign depth mbound =
  Exists (["x0"], repeat_until (fun _ -> random_enf_mon_mfotl_ sign depth mbound 1)
                    (fun f -> List.mem "x0" (free_vars f)) ())

let random_mon_mfotl sign depth mbound =
  Exists (["x0"], repeat_until (fun _ -> random_mon_mfotl_ sign depth mbound 1)
                    (fun f -> List.mem "x0" (free_vars f)) ())

let random_mfotl_trace sign nmax n binsize length =
   let rec aux_arg = function
      0 -> []
    | i when i > 0 -> (Random.int nmax) :: (aux_arg (i-1))
    | _ -> failwith "random_mfotl_trace 1" in
  let rec aux_bin = function
      0 -> []
    | i when i > 0 -> (let pred = choose (sign.fsup @ sign.fcau @ sign.fobs)
                       in (fst pred, aux_arg (snd pred))
                          :: aux_bin (i-1))
    | _ -> failwith "random_mfotl_trace 2" in
  let rec aux ts = function
      0 -> []
    | i when i > 0 -> (let ts' = ts + 1 in
                       (ts', aux_bin (Owl_stats.binomial_rvs ~p:binsize ~n))
                       :: (aux ts' (i-1)))
    | _ -> failwith "random_mfotl_trace 3"
  in aux 0 length

(* Printing: MonPoly *)
   
let rec print_ltl_trace oc = function
    [] -> ()
  | (ts, preds)::t -> (if preds = []
                       then Printf.fprintf oc "@%d\n" ts
                       else List.iter (fun p -> Printf.fprintf oc "@%d %s\n" ts (p ^ "() ")) preds;
                       print_ltl_trace oc t)

let rec print_mfotl_trace oc = function
    [] -> ()
  | (ts, preds)::t -> (Printf.fprintf oc "@%d " ts;
                       List.iter (fun p -> Printf.fprintf oc "%s" ((fst p) ^ "(");
                                           if snd p != [] then
                                             (Printf.fprintf oc "%d" (List.hd (snd p));
                                              List.iter (fun t -> Printf.fprintf oc ", %d" t) (List.tl (snd p)));
                                           Printf.fprintf oc (") ")) preds;
                       Printf.fprintf oc "\n";
                       print_mfotl_trace oc t)

let format_date (d: Unix.tm) =
  let format_2d i =
    (if i < 10 then "0" else "") ^ string_of_int i in 
  "19" ^ string_of_int (d.tm_year) ^ "-"
  ^ format_2d d.tm_mon ^ "-"
  ^ format_2d d.tm_mday ^ " " 
  ^ format_2d d.tm_hour ^ ":"
  ^ format_2d d.tm_min ^ ":"
  ^ format_2d d.tm_sec
  
let rec print_mfotl_trace_json oc = function
    [] -> ()
  | (ts, preds)::t -> (let str_ts = format_date (Unix.gmtime (float_of_int ts)) in
                       Printf.fprintf oc "\t{\n\t\t\"timestamp\": \"%s\",\n\t\t\"predicates\": [\n" str_ts;
                       let l = List.length preds - 1 in
                       List.iteri (fun i p -> Printf.fprintf oc "\t\t\t{\n\t\t\t\t\"name\": \"%s\",\n\t\t\t\t\"occurrences\": [\n\t\t\t\t\t[" (fst p);
                                           if snd p != [] then
                                             (Printf.fprintf oc "%d" (List.hd (snd p));
                                              List.iter (fun t -> Printf.fprintf oc ", %d" t) (List.tl (snd p)));
                                           Printf.fprintf oc ("]\n\t\t\t\t]\n\t\t\t}%s\n") (if i < l then "," else "")) preds;
                       Printf.fprintf oc "\n\t\t]\n\t\n\t}%s\n" (if t = [] then "" else ",");
                       print_mfotl_trace_json oc t)


(* Printing: GREP *)

let rec print_ltl_trace_grep oc = function
    [] -> ()
  | (_, preds)::t -> (List.iter
                         (fun p -> Printf.fprintf oc "%s " p)
                         preds;
                       Printf.fprintf oc "\n";
                       print_ltl_trace_grep oc t)
                    
(* Main *)
    
let get_signature sigfile =
  let ic = open_in sigfile in
  let lexbuf = Lexing.from_channel ic in
  Log_parser.signature Log_lexer.token lexbuf

let _ =
  let i = int_of_string in
  assert (Array.length Sys.argv >= 6);
  Random.self_init();
  let sign = get_signature Sys.argv.(1) in
  match Sys.argv.(2) with
    "mfotl" -> begin match Sys.argv.(3) with
                 "policy" -> (print_formula "" (random_enf_mon_mfotl sign (i Sys.argv.(4)) (float_of_string Sys.argv.(5)));
                              print_newline ())
               | "policy_mon" -> (print_formula "" (random_mon_mfotl sign (i Sys.argv.(4)) (float_of_string Sys.argv.(5)));
                                  print_newline ())
               | "trace"  -> (assert (Array.length Sys.argv >= 9);
                              let oc = open_out (Sys.argv.(8) ^ ".monpoly.trc") in
                              print_mfotl_trace oc ((random_mfotl_trace sign
                                (i Sys.argv.(4)) (i Sys.argv.(5)) (float_of_string Sys.argv.(6)) (i Sys.argv.(7))));
                              close_out oc;)
               | "trace_json" -> (assert (Array.length Sys.argv >= 9);
                                  let trace = random_mfotl_trace sign
                                                                (i Sys.argv.(4))
                                                                (i Sys.argv.(5))
                                                                (float_of_string Sys.argv.(6))
                                                                (i Sys.argv.(7)) in
                                  let oc = open_out (Sys.argv.(8) ^ ".monpoly.json") in
                                  Printf.fprintf oc "[\n";
                                  print_mfotl_trace_json oc trace;
                                  Printf.fprintf oc "]\n";
                                  close_out oc;
                                  let oc = open_out (Sys.argv.(8) ^ ".monpoly.trc") in
                                  print_mfotl_trace oc trace;
                                  close_out oc)
               | _ -> assert false
               end
  | "ltl"   -> begin let sign = ltl_of_mfotl_signature sign in
                     begin match Sys.argv.(3) with
                     | "policy" -> (print_formula "" (random_enf_ltl sign (i Sys.argv.(4)));
                                    print_newline ())
                     | "trace" -> (assert (Array.length Sys.argv >= 8);
                                   let oc1 = open_out (Sys.argv.(7) ^ ".grep.trc") in
                                   print_ltl_trace_grep oc1 ((random_ltl_trace sign
                                     (i Sys.argv.(4)) (float_of_string Sys.argv.(5)) (i Sys.argv.(6))));
                                   close_out oc1;
                                   let oc2 = open_out (Sys.argv.(7) ^ ".monpoly.trc") in
                                   print_ltl_trace oc2 ((random_ltl_trace sign
                                     (i Sys.argv.(4)) (float_of_string Sys.argv.(5)) (i Sys.argv.(6))));
                                   close_out oc2)
                     | _ -> assert false
                     end
               end
  | _ -> assert false

