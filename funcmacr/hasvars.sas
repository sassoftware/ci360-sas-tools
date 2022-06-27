/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : hasvars.sas
/ Author    : Noah Powers
/ Created   : 2019
/ Purpose   : Function-style macro to return true (literally 1) if a dataset has all the
/             variables specified in the user provided list.  If any of the variables
/             provided are not present the macro returns false (literally 0).
/ FuncOutput: 0/1
/ Usage     : %if NOT %hasvars(inds,var1 var2) %then %do ... 
/ Notes     : Non-matching variables will be returned in the global macro
/             variable _nomatch_ .  User parameter error trapping is passed along to 
/             the %match macro.
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/ ds                (positional) The input dataset that will be checked against the list of 
/                     variables
/ varlist           (positional) A space delimited list of variables to look for in DS
/ casesens=N        Y/N Flag to make the comparison case sensitive or not
/
/============================================================================================*/
%macro hasvars(ds,varlist,casesens=N);

  %local varmatch;
  
  %if not %length(&casesens.) %then %let casesens = N ;
  %let casesens = %upcase(%substr(&casesens.,1,1));
 
  %let varmatch=%match(%varlist(&ds.),&varlist.,casesens=&casesens.);
  %if not %length(&_nomatch_.) %then 1;
  %else 0;

%mend;
