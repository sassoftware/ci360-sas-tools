/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Make_Visit_Identity_Lvl.sas
/ Author    : Noah Powers
/ Created   : 2020
/ Purpose   : This macro creates a customer level summary of the discover visit data.
/             
/             The following metrics are generated in the output data:
/             Totals across all visits:
/             - NumVisits 
/
/             Max value across all sessions:
/             - MaxNumVisitsPerSession 
/             Min value across all sessions:
/             - MinNumVisitsPerSession 
/             Avg value across all sessions:
/             - AvgNumVisitsPerSession 
/
/             For each unique value of VISITGROUPVAR:
/             - NumVisits
/             - % of visits in group val
/ FuncOutput: NA
/ Usage     : 
/ Notes     : outrep=LINUX_X86_64
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ inds          The name of the SAS inptut dataset that contains visit_details.
/ identityVar   The name of the column to use as the customer ID.  The default is identity_id
/ start_dttm_var (optional) This is the name of a datetime variable in the input data or a 
/                datetime constant value.  When such a value is provided, only those
/                records with &start_dttm_var. <= session_start_dttm will be included in the 
/                processed data.
/ end_dttm_var   (optional) This is the name of a datetime variable in the input data or a 
/                datetime constant value.  When such a value is provided, only those
/                records with session_start_dttm <= &end_dttm_var. will be included in the 
/                processed data.
/ VisitGroupVar The column in the input dataset that is used to group visit metrics according to
/               the unique levels of this variable.
/ outds         The name of the output SAS dataset to create at the IDENTITYVAR level
/ outdsOpts     (default compress=YES) optional dataset options to apply to the output 
/               dataset created.
/============================================================================================*/
%macro Make_Visit_Identity_Lvl(inds          =,
                              identityVar    =identity_id,
                              start_dttm_var =,
                              end_dttm_var   =,
                              VisitGroupVar  =,
                              outds          =,
                              outdsOpts      =%str(compress=YES)) ;

  %local VisitGroups NumVisitGroups i group NumVisitList pctVisitList whereStmt InDsLib InDsName inViya libEngine ;
  
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
    %let whereStmt = %str(&start_dttm_var. <= visit_dttm <= &end_dttm_var.) ;
  %else %if %length(&start_dttm_var.) > 0 %then 
    %let whereStmt = %str(&start_dttm_var. <= visit_dttm) ;
  %else %if %length(&end_dttm_var.) > 0 %then 
    %let whereStmt = %str(visit_dttm <= &end_dttm_var.) ;
  %else %let whereStmt = 1 ;
  
  %if %length(&VisitGroupVar.) %then %do ;
    %if (&inViya. = 0) %then %do ;
      proc sql noprint ;
        select distinct &VisitGroupVar. into: VisitGroups separated by " " from &inds. (where=(&WhereStmt.)) ;
      quit ;
    %end ;
    %else %do ;
      data &InDsLib..VisitGroupList_ ;
        set &inds. (keep=&VisitGroupVar. &start_dttm_var. visit_dttm &end_dttm_var. where=(&WhereStmt.)) ;
        by &VisitGroupVar. ;
        if first.&VisitGroupVar. ;
      run ;
      
      proc sql noprint ;
        select &VisitGroupVar. into: VisitGroups separated by " " from &InDsLib..VisitGroupList_ ;
      quit ;
    %end ;
  %end ;

  %let NumVisitGroups = %words(&VisitGroups.) ;

  %if (&inViya. = 0) %then %do ;
    ** Sum to identity and session **;
    proc means data=&inds. (where=(&whereStmt.)) nway noprint ;
      by &identityVar. ;
      class session_id ;
      var sequence_no ;
      output out=&InDsLib..visit_session_ (drop=_type_ rename=(_freq_=NumVisits)) sum= ;
    run ;
  
    ** Sum across sessions to identity **;
    proc means data=&InDsLib..visit_session_ nway noprint ;
      by &identityVar. ;
      var NumVisits ;
      output out=&InDsLib..visit_session_sum_ (compress=YES drop=_type_ _freq_) 
        sum(NumVisits)= 
        max(NumVisits)=MaxNumVisitsPerSession
        min(NumVisits)=MinNumVisitsPerSession  
        mean(NumVisits)=AvgNumVisitsPerSession ;
    run ;
  
    ** Sum to page group level **;
    proc means data=&inds. (where=(&whereStmt.)) nway noprint ;
      by &identityVar. ;
      class &VisitGroupVar. ;
      var sequence_no ;
      output out=&InDsLib..visit_grp_ (drop=_type_ rename=(_freq_=NumVisits)) sum=;
    run ;
  
    ** Transpose groups to identity **;
    proc transpose data=&InDsLib..visit_grp_ out=&InDsLib..visit_grp_T1_ prefix=NumVisit_; 
      by &identityVar. ;
      id &VisitGroupVar. ;
      var NumVisits ;
    run ;
  %end ;
  %else %do ;
    %CAS_Proc_Means(inds           =&inds.,
                    GroupVars      =&identityVar. session_id,
                    InDsWhere      =%str(&WhereStmt.),
                    Vars           =sequence_no,
                    AggTypeList    =n,
                    AggVarNames    =NumVisits,
                    outds          =&InDsLib..visit_session_
                    ) ;
 
    %CAS_Proc_Means(inds           =&InDsLib..visit_session_,
                    GroupVars      =&identityVar.,
                    InDsWhere      =,
                    Vars           =NumVisits,
                    AggTypeList    =sum max min mean,
                    AggVarNames    =NumVisits MaxNumVisitsPerSession MinNumVisitsPerSession AvgNumVisitsPerSession,
                    outds          =&InDsLib..visit_session_sum_
                    ) ;     
                    
    %CAS_Proc_Means(inds           =&inds.,
                    GroupVars      =&identityVar. &VisitGroupVar.,
                    InDsWhere      =%str(&WhereStmt.),
                    Vars           =sequence_no,
                    AggTypeList    =n,
                    AggVarNames    =NumVisits,
                    outds          =&InDsLib..visit_grp_
                    ) ;     
                    
    proc cas ;
      transpose.transpose / 
        table={
              name="visit_grp_",
              caslib="&InDsLib.",
              groupBy={"&identityVar."}
              },
        id={"&VisitGroupVar."},
        prefix="NumVisit_",
        casOut={name="visit_grp_T1_", caslib="&InDsLib.", replace=true},
        transpose={"NumVisits"} ;
    quit ;                      
  %end ;
  
  %do i = 1 %to &NumVisitGroups. ;
    %let group = %scan(&VisitGroups.,&i.,%str( )) ;
    %let NumVisitList = &NumVisitList. NumVisit_&group. ;
    %let pctVisitList = &pctVisitList. pctVisit_&group. ;
  %end ;

  data &outds. (&outdsOpts.) ;
    merge &InDsLib..visit_session_sum_  (in=insess)
          &InDsLib..visit_grp_T1_       (in=in1 drop=_name_)  
          ;
    by &identityVar. ;
    if not (first.&identityVar. and last.&identityVar.) then abort ;

    array numvis   (&NumVisitGroups.) &NumVisitList. ;
    array pctVis   (&NumVisitGroups.) &pctVisitList. ;    
    
    do i = 1 to &NumVisitGroups. ;
      if NumVisits > 0 AND numVis(i) ne . then pctVis(i) = numVis(i) / NumVisits ;
    end ;

    array nums (*) _numeric_ ;
    do i = 1 to dim(nums) ;
      if nums(i) = . then nums(i) = 0 ;
    end ;

    drop i ;
    label NumVisits               = "Total Num Visits"
          MaxNumVisitsPerSession  = "Max Num Visits per Session"
          MinNumVisitsPerSession  = "Min Num Visits per Session"
          AvgNumVisitsPerSession  = "Avg Num Visits per Session"
          %do i = 1 %to &NumVisitGroups. ;
            %let group = %scan(&VisitGroups.,&i.,%str( )) ;
            NumVisit_&group. = "Num Visits from group=&group."
            pctVisit_&group. = "% Visits from group=&group." 
          %end ;
    ;
  run ;

  proc datasets library=&InDsLib. nolist ;
    delete visit_session_ VisitGroupList_ visit_session_sum_ visit_grp_ visit_grp_T1_ ;
  quit ;

%mend ;
