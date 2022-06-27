/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Tagsort_Inmem.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2020 March
/ LastModBy : Noah Powers
/ LastModDt : 03.20.2020
/ Purpose   : Implement the logic of TAGSORT option from proc sort without explicitly
/             using it so that we can leverage more than 2G of Memory when sorting. The use
/             case is a dataset with both many records and wide records relative to the sort 
/             keys.  Discover and Engage data often fits this profile. 
/ FuncOutput: N/A
/ Usage     :
/ Notes     : This elegant and efficient approach came from this article:
/               https://www.lexjansen.com/nesug/nesug12/cc/cc36.pdf
/             - Could leverage dictionary.tables to determine compression status of the 
/               input data and automatically enforce that on the output data.
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ inds         Name of the SAS dataset (including library, if not work) to be sorted
/ OutDs        Name of the output sorted SAS dataset (including library, if not work) 
/ outdsOpts     Any dataset options to be associated with the output dataset created.  
/ SortOpts    (optional) If supplied this string is added to the proc sort options when
/               sorting the source by variable data view
/ sortByVars   This is a space delimited list of the sort or by variables or TAGS
/============================================================================================*/
%macro Tagsort_InMem(inds=,
                     outds=&inds.,
                     outdsopts=,
                     sortOpts=,
                     sortbyVars=
                     ) ;

  %local fullstimerOption ;
 
  %let fullstimerOption = %sysfunc(getoption(FULLSTIMER));
  options FULLSTIMER ;

  %if "%scan(&inds.,1,%str(.))"="&inds." %then %do ;
    %let inlib = WORK ;
    %let indat = &inds. ;
  %end ;
  %else %do ;
    %let inlib = %scan(&inds.,1,%str(.)) ;
    %let indat = %scan(&inds.,2,%str(.)) ;
  %end ;

  data _tempvw_ / view=_tempvw_ ;
    set &inds. (keep=&sortbyVars.) ;
     _obs_ = _N_ ;
  run ;

  proc sort &sortOpts. data=_tempvw_ out=_tmpsrt_; 
    by &sortbyVars. ;
  run ;
  
  proc copy in=&inlib. out=WORK memtype=data ;
    select &indat. ; 
  run ;

  proc datasets library=WORK nolist ;
    change &indat.=_tmp ;
  quit ;

  sasfile WORK._tmpsrt_.data open ;
  sasfile WORK._tmp.data open ;

  Data &outds. (&outdsOpts.) ;
    set _tmpsrt_ (keep=_obs_) ;
    set _tmp point=_obs_ ;
  run ;
  
  sasfile WORK._tmpsrt_ close ;
  sasfile WORK._tmp close ;

  proc datasets library=WORK nolist ;
    delete _tmpsrt_ _tmp / memtype=data ;
  quit ;

  options &fullstimerOption. ;
%mend ;
