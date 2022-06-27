/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : Is_Valid_SAS_Name.sas
/ Author    : Noah Powers
/ Created   : 2019
/ Purpose   : Function style macro that returns 0/1 depending on whether the string/name
/             passed is a valid SAS name or not.
/ FuncOutput: 0/1
/ Usage     :
/ Notes     :
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ Name               (pos) A string or name that will be checked to see if it is a valid
/                     SAS name or not.
/ -------------------------------------------------------------------------------------
/============================================================================================*/
%macro Is_Valid_SAS_Name(name) ;

  %local maxlen rc V1 V2;

  %let maxlen = 32 ;
  %if &sysver. < 7 %then %let maxlen = 8 ;
  %let name = %upcase(&name.);

  %if %length(&name.) > &maxlen. or %length(&name.) = 0 %then %let rc = 0 ;
  %else %do ;
    %let V1 = %sysfunc(verify(&name.,_ABCDEFGHIJKLMNOPQRSTUVWXYZ)) ;
    %let V2 = %sysfunc(verify(&name.,_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789)) ;

    %if &V1 = 0 %then %let rc = 1;  %** entire string contained only _A-Z **;
    %else %if &V1 = 1 %then %let rc = 0 ;  %** string did not start with _A-Z **;
    %else %if &V2 = 0 %then %let rc = 1;   %** string started with _A-Z with remainder containing only _A-Z0-9 **;
    %else %let rc = 0 ; %** string contained something other than _A-Z _A-Z0-9 ...**;
  %end;

  &rc.

%mend ;
