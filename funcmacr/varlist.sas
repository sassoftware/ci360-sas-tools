/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : VarList.sas
/ Author    : Noah Powers 
/ Created   : 2019
/ Purpose   : Function-style macro to return the list of variables in a SAS dataset
/             (space delimited).  The variable names will be provided in upper case and in 
/             the order in which the are in the dataset.
/ Usage     : %let Varlist = %varList(WORK.mydata) 
/ Notes     : The sort order of the variables is NOT necessarily alphabetical
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/ inds              (positional) The input dataset.
/============================================================================================*/
%macro varlist(inds);

  %local indsid rc numVars i varlist;
  %let indsid = %sysfunc(open(&inds.,is));
    
  %if (&indsid. NE 0) %then %do;
    %let numVars = %sysfunc(attrn(&indsid.,NVARS));
   
    %do i = 1 %to &numVars;      
      %let varlist = &varlist. %sysfunc(varname(&indsid.,&i.));
    %end;

    %let rc = %sysfunc(close(&indsid.));
  %end;
  %else %do;
    %put %upcase(Error): (varlist) Input dataset (&inds.) not opened due to the following reason:;
    %put %sysfunc(sysmsg());
  %end;
    
  &varlist.

%mend;
