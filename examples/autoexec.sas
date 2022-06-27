/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
*/

options source2 source NOXWAIT XSYNC XMIN ;
options mprint NOQUOTELENMAX msglevel=i ;

%let CC_DIR = D:\Temp\Noah\macros ;

filename FuncMcro "%superq(CC_DIR)\funcmacr" ;
filename ProcMcro "%superq(CC_DIR)\procmacr" ;

** Add funcmacr and procmarc to end of sasautos list **;
options mautosource insert = (sasautos = ( FuncMcro ProcMcro )) ;
