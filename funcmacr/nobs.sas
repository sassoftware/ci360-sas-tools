/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : nobs.sas
/ Author    : Noah Powers 
/ Created   : 2019
/ Purpose   : Function-style macro to return the number of observations in a
/             dataset or data view.
/ Usage     : %put %nobs(sashelp.cars)           
/             %put %nobs(sashelp.vslib) 
/ Notes     : If the dataset is a view then to count the number of observations, 
/             a forced read is done of the dataset using NLOBSF which can be slow 
/             for large datasets. 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/  inds                (positional) The input dataset name.  
/============================================================================================*/
%macro nobs(inds);

  %local nobs attr dsid rc;

  %let attr = NOBS ;
  %let dsid = %sysfunc(open(&inds.));
 
  %if (&dsid. NE 0) %then %do;
    %if (%sysfunc(attrc(&dsid.,MTYPE)) = VIEW) %then 
      %let attr = NLOBSF ;
    
    %let nobs = %sysfunc(attrn(&dsid.,&attr.));
    %let rc = %sysfunc(close(&dsid.));    
    %if (&nobs. < 0) %then %let nobs = 0;
    
    &nobs
  %end;
  %else %do;
    %put %upcase(Error): (nobs) Input dataset &inds. not opened due to the following reason:;
    %put %sysfunc(sysmsg());
  %end;
    
%mend;