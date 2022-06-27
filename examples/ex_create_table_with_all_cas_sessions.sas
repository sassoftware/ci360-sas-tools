/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

** Create WORK.CasSessions that contains one row for each CAS session **;
proc cas;
  session.listSessions result=res1 status=stat ; 
  run ;
  table1 = findtable(res1) ;
  saveresult table1 dataout=work.casSessions ;
quit;

  