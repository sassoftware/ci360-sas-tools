/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : VarType.sas
/ Author    : Noah Powers
/ Created   : 2019
/ Purpose   : Function-style macro to return the type of a variable.  The returned value
/             will either be C or N depending on wether the type is character or numeric
/             respectively.
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
%macro vartype(inds,var);

  %local dsid varnum vartype rc ;

  %let dsid = %sysfunc(open(&inds.,is));
  
  %if (&dsid. NE 0) %then %do;
    %let varnum = %sysfunc(varnum(&dsid.,&var.));
    %if (&varnum. < 1) %then %put %upcase(Error): (VARTYPE) Variable &var. not in dataset &inds. ;
    %else %let vartype = %sysfunc(vartype(&dsid.,&varnum.)) ;
    %let rc = %sysfunc(close(&dsid.));
    
    &vartype.
  %end;
  %else %do;
    %put %upcase(Error): (VARTYPE) Dataset &inds. not opened due to the following reason:;
    %put %sysfunc(sysmsg());
  %end ;

%mend;
