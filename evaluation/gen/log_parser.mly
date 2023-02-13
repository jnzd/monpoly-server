/*
 * This file is part of MONPOLY.
 *
 * Copyright © 2011 Nokia Corporation and/or its subsidiary(-ies).
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
 */



%{
  open Gen_types

  let empty_signature =
    { fcau = []; fsup = []; fobs = []}

    
  let update_signature sign pred =
    match pred.lvl with
      Cau -> { sign with fcau = (pred.name, pred.arity)::sign.fcau }
    | Sup -> { sign with fsup = (pred.name, pred.arity)::sign.fsup }
    | Obs -> { sign with fobs = (pred.name, pred.arity)::sign.fobs }

  let make_predic name arity lvl =
    { name; arity; lvl }
 

%}




%token AT LPA RPA COM PLS MNS SC
%token <string> STR
%token EOF 
%token ERR

%start signature
%type <Gen_types.foltl_signature> signature

%%


signature:
      | predicate signature         { update_signature $2 $1 }
      |                             { empty_signature }

predicate:
      | STR LPA fields RPA      { make_predic $1 (List.length $3) Obs }
      | STR LPA fields RPA PLS  { make_predic $1 (List.length $3) Cau }
      | STR LPA fields RPA MNS  { make_predic $1 (List.length $3) Sup }

fields:
      | STR COM fields          { $1::$3 }
      | STR                     { [$1] }
      |                         { [] }
