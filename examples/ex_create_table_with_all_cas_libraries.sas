/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

** Create table WORK.casLibs that contains all cas libraries of type PATH **;
proc cas;
  table.caslibinfo result=res1 status=stat / srcType='PATH';
  verbose=TRUE;
  run;

  table1 = findtable(res1);
  saveresult table1 dataout=work.CASLIBS;
  run;
quit;