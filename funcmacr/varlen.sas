/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : VarLen.sas
/ Author    : Noah Powers
/ Created   : 2019
/ Purpose   : Function-style macro to return the length of a SAS variable.  By default 
/             the length of character variables are returned with a leading dollar sign
/             $ unless the user specifies INCLUDEDOLLARSIGN = N.
/ FuncOutput:
/ Usage     : data newdata
/               set olddata 
/               length newvar %varlen(olddata,oldvar) 
/               ...
/             run 
/
/ Notes     : This is particularly useful for cloning variables.  That is when a new
/             variable needs to be created that is the same type and length as some
/             pre-existing variable.  
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/ inds              (positional) Name of the dataset that contains the variable to be
/                    analyzed
/ var               (positional) Name of the variable to return the length of.
/ IncludeDollarSign  Y/N This is a flag that indicates if a leading dollar
/                     sign should be included on the output or not.  The default is Y
/============================================================================================*/
%macro varlen(inds,var,IncludeDollarSign=Y);

  %local dsid varnum varlen rc ;

  %if NOT %length(&IncludeDollarSign.) %then %let IncludeDollarSign = Y ;
  %let IncludeDollarSign = %substr(&IncludeDollarSign.,1,1) ;
  
  %let dsid = %sysfunc(open(&inds.,is));
  
  %if (&dsid. NE 0) %then %do;
    %let varnum = %sysfunc(varnum(&dsid.,&var.));
    %if (&varnum. < 1) %then %put %upcase(Error): (VARLEN) Variable &var. not in dataset &inds. ;
    %else %let varlen = %sysfunc(varlen(&dsid.,&varnum.)) ;
    %let rc = %sysfunc(close(&dsid.));
    
    %if "%vartype(&inds.,&var.)" = "C" and "&IncludeDollarSign." = "Y" %then %let varlen=$&varlen;
    &varlen.
  %end;
  %else %do;
    %put %upcase(Error): (VARLEN) Dataset &inds. not opened due to the following reason:;
    %put %sysfunc(sysmsg());
  %end ;

%mend ;
