/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : VarFormat.sas
/ Author    : Noah Powers
/ Created   : 2022
/ Purpose   : Function-style macro to return the format of a variable.  
/ Usage     : %if %vartype(mylib.mydata,myvar) ne N %then %do..      
/ Notes     : 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/ inds              (positional) Name of the dataset that contains the variable to be analyzed                   
/ var               (positional) Name of the variable to return the type of.
/============================================================================================*/
%macro varformat(inds,var);

  %local dsid varformat varnum rc ;

  %let dsid = %sysfunc(open(&inds.,is));
  
  %if (&dsid. NE 0) %then %do;
    %let varnum = %sysfunc(varnum(&dsid.,&var.));
    %if (&varnum. < 1) %then %put %upcase(Error): (VARFORMAT) Variable &var. not in dataset &inds. ;
    %else %let varformat = %sysfunc(varfmt(&dsid.,&varnum.)) ;
    %let rc = %sysfunc(close(&dsid.));
    
    &varformat.
  %end;
  %else %do;
    %put %upcase(Error): (VARFORMAT) Dataset &inds. not opened due to the following reason:;
    %put %sysfunc(sysmsg());
  %end ;

%mend;
