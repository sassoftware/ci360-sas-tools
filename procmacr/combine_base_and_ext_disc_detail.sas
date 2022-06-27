/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Combine_Base_and_Ext_Disc_Detail.sas
/ Author    : Noah Powers
/ Created   : 2020
/ Purpose   : Merge Discover detail data base and corresponding _EXT table into one table:
/
/               input: &inlib..<name>_details     (base)
/                      &inlib..<name>_details_ext (ext)
/               output: &outlib..<name>_details_all - All records in base (with matching data from EXT)
/                       &outlib..<name>_ext_only    - All EXT records with no match in base
/
/             where all values found in the TABLES macro parameter will each be used for <name> above. 
/             Note that the unique record count totals across the input datasets will be the same 
/             total across the output datasets.  Typically < 1% of EXT records are unmatched.
/             
/             Each EXT table info is total time for a entity (e.g. page or session etc) time information.
/             Which is captured separately from the streamable data elements in the detail tables.  
/              
/ FuncOutput: NA
/ Usage     : 
/ Notes     : - Need to expand to include the Media details and extension
/             - could add a proc datasets provided no errors from datastep,  to delete the original 
/               data to save disk space.
/             - Ask/think about is it possible to deduce the information for the detail
/               data records that are missing the corresponding ext record?
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ inlib         Required - The SAS library name where the discover detail tables reside to be
/               combined
/ TimeStampDs   (&inlib..timestamps) The timestamps dataset created by read_udm_data.sas
/                that contains the list of datasets to read
/ tables        These are the names, expected in the form of <name> in the above description.  The 
/               defalut value is page session
/ sortData (Y/N) flag to sort the data prior to merging.  The default is N
/ CompressOutput (Y/N) The default is Y and this will result in the output SAS datasets
/                getting compressed. 
/ outlib         Required - The SAS library name where the output datasets should be saved.
/                If the user does not speicify this pararmeter, the input library is used
/============================================================================================*/
%macro Combine_Base_and_Ext_Disc_Detail(inlib         =,
                                        TimeStampsDs  =&inlib..timestamps,
                                        names         =%str(page session),
                                        sortData      =N,
                                        CompressOutput=Y,
                                        outlib        =&inlib.) ;

  %local NumNames i nm sortVars1 sortVars2 extKeepList1 extKeepList2 lastSortVar NumObsNOTinExt dsopt ;

  %if NOT %length(&outlib.) %then %let outlib = &inlib. ;
  %let names = %upcase(%match(%upcase(&names.),PAGE SESSION)) ;
  %let NumNames = %words(&names.) ;
  
  %if not %length(&CompressOutput.) %then %let CompressOutput = Y ;
  %let CompressOutput = %upcase(%substr(&CompressOutput.,1,1)) ;
  %if "&CompressOutput." = "Y" %then %let dsopt = %str(compress=YES) ;

  %do i = 1 %to &NumNames. ;
    %let nm = %upcase(%scan(&names.,&i.,%str( ))) ;
   
    %if "&nm." = "PAGE" %then %do ;
      %let sortVars&i. = session_id detail_id ;
      %let extKeepList&i. = active_sec_spent_on_page_cnt seconds_spent_on_page_cnt ;
    %end ;
    %else %if "&nm." = "SESSION" %then %do ;
      %let sortVars&i. = session_id  ;
      %let extKeepList&i. = %cmpres(active_sec_spent_in_sessn_cnt last_session_activity_dttm 
           last_session_activity_dttm_tz seconds_spent_in_session_cnt session_expiration_dttm 
           session_expiration_dttm_tz);
    %end ;

    %*put i = &i. ;
    %*put nm = &nm. ;
    %*put sortvars&i. = &&sortvars&i. ;
    %*put extKeepList&i. = &&extKeepList&i. ;
  %end ;

  %if ("%substr(&sortData.,1,1)" = "Y") %then %do ;
  
    %do i = 1 %to &NumNames. ;

      %let nm = %scan(&Names.,&i.,%str( )) ;

      %Tagsort_InMem(inds=&inlib..&nm._details,
                    outdsOpts=%str(&dsopt.),
                    sortbyVars=&&sortVars&i.,
                    sortOpts=sortsize=25G) ;

      %Tagsort_InMem(inds=&inlib..&nm._details_ext,
                    outdsOpts=%str(&dsopt.),
                    sortbyVars=&&sortVars&i.,
                    sortOpts=sortsize=25G) ;

    %end ;

  %end ;


  %do i = 1 %to &NumNames. ;

    %let nm = %scan(&Names.,&i.,%str( )) ;
    %let lastSortVar = %scan(&&sortVars&i.,%words(&&sortVars&i.),%str( )) ;

    *------------------------------------------------------------------------------------*;
    * Combine &nm._details and &nm._details_ext into &nm._details_all                    *;
    * Recommend deleting the original files to save space once successfully executed     *;
    *------------------------------------------------------------------------------------*;

    data &outlib..&nm._details_all      (&dsopt.) 
         &outlib..&nm._details_ext_only (&dsopt. keep=&&sortVars&i. &&extKeepList&i.);
      merge &inlib..&nm._details     (in=inmain)
            &inlib..&nm._details_ext (in=in_ext keep=&&sortVars&i. &&extKeepList&i.)
      ;
      by &&sortVars&i. ;
      length inExt 3. ;
      *if not (first.&lastSortVar. and last.&lastSortVar.) then abort ;
      inExt = (in_ext) ;
      if (inext and (not inmain)) then output &outlib..&nm._details_ext_only ;
      if inmain then output &outlib..&nm._details_all ;
    run ;

    %if %nobs(&outlib..&nm._details_ext_only) <= 0 %then %do ;
      proc datasets library=&outlib. nolist ;
        delete &nm._details_ext_only / memtype=data ;
      quit ;
    %end ;

    proc sql noprint ;
      select count(*) into: NumObsNOTinExt from &outlib..&nm._details_all where inExt = 0 ;
    quit ;

    %put %cmpres(W%upcase(arning): %trim(&NumObsNOTinExt.) of %trim(%nobs(&outlib..&nm._details_all)) records 
        (%sysevalf(100*&NumObsNOTinExt./%nobs(&outlib..&nm._details_all)) percent) not found in EXT table) ;  

    data newData ;
      set &TimeStampsDs. (where=(upcase(entityName)="&nm._DETAILS")) ;
      entityName = "&nm._DETAILS_ALL" ;
    run ;

    data &TimeStampsDs. ;
      set &TimeStampsDs. (where=(entityName NE "&nm._DETAILS_ALL"))
          newData ;
      by entityName ;
    run ;

  %end ;

%mend ;


