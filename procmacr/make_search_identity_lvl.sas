/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Make_Search_Identity_Lvl.sas
/ Author    : Noah Powers
/ Created   : 2022
/ Purpose   : This macro creates a customer level summary of the discover search_results
/             data. 
/             
/             The following metrics are generated in the output data:
/             Totals across all search records:
/             - NumSearches 
/
/             Avg value across all sessions:
/             - AvgSearchesPerSession 
/             - MinSearchesPerSession 
/             - MaxSearchesPerSession 
/
/             For each unique value of GROUPVAR:
/             - NumSearches (number of searches in each value of groupvar)
/             - PctSearches (percent of searches in each value of groupvar)
/
/ FuncOutput: NA
/ Usage     : 
/ Notes     : The values of the groupVar should NOT contain spaces.  This code creates
/             a space delimited list of the unique groupVar values.
/
/             NJP 4.19.22 Perhaps adding group metrics within session would be valuable 
/             It would be interesting to have info on how many times user clicked on
/             search results or went back to a search results page.  Currently not tracked in
/             search events in CI360
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ inds          The name of the SAS inptut dataset that contains document_details data
/ GroupVar      (optional) The column in the input dataset that is used to group similar
/               documents according to the unique levels of this variable.
/ identityVar   The name of the column to use as the customer ID.  The default is identity_id
/ start_dttm_var (optional) This is the name of a datetime variable in the input data or a 
/                datetime constant value.  When such a value is provided, only those
/                records with &start_dttm_var. <= session_start_dttm will be included in the 
/                processed data.
/ end_dttm_var   (optional) This is the name of a datetime variable in the input data or a 
/                datetime constant value.  When such a value is provided, only those
/                records with session_start_dttm <= &end_dttm_var. will be included in the 
/                processed data.
/ outds         The name of the output SAS dataset to create at the IDENTITYVAR level
/ outdsOpts     (default compress=YES) optional dataset options to apply to the output 
/               dataset created.
/============================================================================================*/
%macro Make_Search_Identity_Lvl(inds           =,
                                identityVar    =identity_id,
                                start_dttm_var =,
                                end_dttm_var   =,
                                GroupVar       =,
                                outds          =,
                                outdsOpts      =%str(compress=YES)) ;

  %local InDsLib InDsName inViya libEngine whereStmt Groups NumGroups i group NumList pctList ;
        
  %let InDsLib  = %scan(&InDs.,1,%str(.)) ;
  %let InDsName = %scan(&InDs.,2,%str(.)) ;

  %if not %length(&InDsName.) %then %do ;
    %let InDsLib  = WORK ;
    %let InDsName = &InDs. ;
  %end ;        
  
  %if ("%substr(&sysvlong.,1,1)" = "V") %then %do ;
    proc contents data=&inds. out=_conts_ noprint ;
    run ;
    
    proc sql noprint ;
      select distinct engine into: libEngine separated by "*" from _conts_ ;
    quit ;
    
    %if "%upcase(&libEngine.)" = "CAS" %then %let inViya = 1 ;      
      %else %let inViya = 0 ;
  %end ;
  %else %let inViya = 0 ;

  %if %length(&start_dttm_var.) > 0 AND %length(&end_dttm_var.) > 0 %then 
    %let whereStmt = %str(&start_dttm_var. <= search_results_dttm <= &end_dttm_var.) ;
  %else %if %length(&start_dttm_var.) > 0 %then 
    %let whereStmt = %str(&start_dttm_var. <= search_results_dttm) ;
  %else %if %length(&end_dttm_var.) > 0 %then 
    %let whereStmt = %str(search_results_dttm <= &end_dttm_var.) ;
  %else %let whereStmt = 1 ;
  
  %if %length(&GroupVar.) %then %do ;
    %if (&inViya. = 0) %then %do ;
      proc sql noprint ;
        select distinct &GroupVar. into: Groups separated by " " from &inds. (where=(&WhereStmt.)) ;
      quit ;
    %end ;
    %else %do ;
      data &InDsLib..GroupList_ ;
        set &inds. (keep=&GroupVar. &start_dttm_var. &end_dttm_var. search_results_dttm where=(&WhereStmt.)) ;
        by &GroupVar. ;
        if first.&GroupVar. ;
      run ;
      
      proc sql noprint ;
        select &GroupVar. into: Groups separated by " " from &InDsLib..GroupList_ ;
      quit ;
    %end ;
  %end ;

  %let NumGroups = %words(&Groups) ;

  %if (&inViya. = 0) %then %do ;
    ** Sum to identity and session **;
    proc means data=&inds. (where=(&WhereStmt.)) nway noprint ;
      by &identityVar. ;
      class session_id ;
      var load_dttm ;
      output out=&InDsLib..search_session_ (drop=_type_ _freq_) N=NumSearches ;
    run ;
  
    ** Sum across sessions to identity **;
    proc means data=&InDsLib..search_session_ nway noprint ;
      by &identityVar. ;
      var NumSearches ;
      output out=&InDsLib..search_session_sum_ (compress=YES drop=_type_ _freq_)         
        sum(NumSearches)=NumSearches 
        mean(NumSearches)=AvgNumSearchesPerSession 
        min(NumSearches)=MinNumSearchesPerSession 
        max(NumSearches)=MaxNumSearchesPerSession ;
    run ;
  %end ;
  %else %do ;
  
    %CAS_Proc_Means(inds           =&inds.,
                    GroupVars      =&identityVar. session_id,
                    InDsWhere      =%str(&WhereStmt.),
                    Vars           =load_dttm,
                    AggTypeList    =n,
                    AggVarNames    =NumSearches,
                    outds          =&InDsLib..search_session_
                    ) ;
                    
    %CAS_Proc_Means(inds           =&InDsLib..search_session_,
                    GroupVars      =&identityVar.,
                    InDsWhere      =,
                    Vars           =NumSearches,
                    AggTypeList    =sum mean min max,
                    AggVarNames    =NumSearches 
                                    AvgNumSearchesPerSession
                                    MinNumSearchesPerSession
                                    MaxNumSearchesPerSession,
                    outds          =&InDsLib..search_session_sum_
                    ) ;                    

  %end ;
  
  %if (&NumGroups. >  0) AND (&inViya. = 0) %then %do ;
    ** Sum to group level **;
    proc means data=&inds. (where=(&WhereStmt.)) nway noprint ;
      by &identityVar. ;
      class &GroupVar. ;
      var load_dttm ;
      output out=&InDsLib..search_grp_ (drop=_type_ _freq_) N=NumSearches ;
    run ;
  
    ** Transpose groups to identity **;
    proc transpose data=&InDsLib..search_grp_ out=&InDsLib..search_grp_T1_ prefix=NumSearches_; 
      by &identityVar. ;
      id &GroupVar. ;
      var NumSearches ;
    run ;
  
  %end ;
  %else %if (&NumGroups. >  0) AND (&inViya. = 1) %then %do ;
  
    %CAS_Proc_Means(inds           =&inds.,
                    GroupVars      =&identityVar. &GroupVar.,
                    InDsWhere      =%str(&WhereStmt.),
                    Vars           =load_dttm,
                    AggTypeList    =N,
                    AggVarNames    =NumSearches,
                    outds          =&InDsLib..search_grp_
                    ) ;
                    
    proc cas ;
      transpose.transpose / 
        table={
              name="search_grp_",
              caslib="&InDsLib.",
              groupBy={"&identityVar."}
              },
        id={"&GroupVar."},
        prefix="NumSearches_",
        casOut={name="search_grp_T1_", caslib="&InDsLib.", replace=true},
        transpose={"NumSearches"} ;
    quit ;  
  
  %end ;
  
  %do i = 1 %to &NumGroups. ;
    %let group   = %scan(&Groups.,&i.,%str( )) ;
    %let pctList = &pctList. pctSearches_&group. ;   
    %let NumList = &NumList. NumSearches_&group. ;
  %end ;

  data &outds. (&outdsOpts.) ;
    merge &InDsLib..search_session_sum_ (in=insess)
          %if (&NumGroups. > 0) %then %do ;
            &InDsLib..search_grp_T1_    (in=in1 drop=_name_)             
          %end ;;
    by &identityVar. ;
    if not (first.&identityVar. and last.&identityVar.) then abort ;
    
    %if (&NumGroups. > 0) %then %do ;
      if not (in1) then abort ;
    %end ;
    
    %if (&NumGroups. > 0) %then %do ;
      array num (&NumGroups.) &NumList. ;      
      array pct (&NumGroups.) &pctList. ;    
  
      do i = 1 to &NumGroups. ;
        if NumSearches > 0 AND num(i) ne . then pct(i) = num(i) / NumSearches ;
      end ;
    %end ;
    
    array nums (*) _numeric_ ;
    do i = 1 to dim(nums) ;
      if nums(i) = . then nums(i) = 0 ;
    end ;
    
    drop i ;
    
    format 
      %do i = 1 %to &NumGroups. ;
        %let group = %scan(&Groups.,&i.,%str( )) ;
        NumSearches_&group. comma10.           
        pctSearches_&group. percent12.2 
      %end ;
    ;
    
    label NumSearches                         = "Total Searches"          
          AvgNumSearchesPerSession            = "Avg Num Searches per Session"         
          MinNumSearchesPerSession            = "Min Num Searches per Session"         
          MaxNumSearchesPerSession            = "Max Num Searches per Session"         
          %do i = 1 %to &NumGroups. ;
            %let group = %scan(&Groups.,&i.,%str( )) ;
            NumSearches_&group. = "Num Searches in Group=&group."           
            pctSearches_&group. = "% Searches in Group=&group."            
          %end ;
    ;
  run ;
  
  proc datasets library=&InDsLib. nolist ;
    delete GroupList_ search_session_ search_session_sum_ search_grp_ search_grp_T1_ ;
  quit ;

  proc datasets library=WORK nolist ;
    delete _conts_ ;
  quit ;

%mend ;