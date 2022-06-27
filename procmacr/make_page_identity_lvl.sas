/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Make_Page_Identity_Lvl.sas
/ Author    : Noah Powers
/ Created   : 2020
/ Purpose   : This macro creates a customer level summary of the discover PAGE (plus EXT)
/             data. Note the page input data is expected to have the EXT columns included.
/             
/             The following metrics are also generated in the output data:
/             Totals across all pages:
/             - NumPages seconds_spent_on_page_cnt active_sec_spent_on_page_cnt
/
/             Max value across all sessions:
/             - MaxNumPagesPerSession MaxSeconds_spent_on_page_PerSess MaxActive_sec_on_page_perSess
/             Min value across all sessions:
/             - MinNumPagesPerSession Minseconds_spent_on_page_PerSess MinActive_sec_on_page_perSess
/             Avg value across all sessions:
/             - AvgNumPagesPerSession Avgseconds_spent_on_page_PerSess AvgActive_sec_on_page_perSess
/
/             For each unique value of PAGEGROUPVAR:
/             - NumPages
/             - seconds_spent_on_page_cnt 
/             - active_sec_spent_on_page_cnt
/             - % of pages in group val
/             - % of seconds spent on group val
/             - % of active seconds spent on group val
/ FuncOutput: NA
/ Usage     : 
/ Notes     : The values of the PageGroupVar should NOT contain spaces.  This code creates
/             a space delimited list of the unique PageGroupVar values.
/
/             NJP 4.19.22 Perhaps adding group metrics within session would be valuable 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ inds          The name of the SAS inptut dataset that contains merged session_details
/               and session_details_ext columns in one dataset.
/ PageGroupVar  The column in the input dataset that is used to group pages according to
/               the unique levels of this variable.
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
%macro Make_Page_Identity_Lvl(inds           =,
                              identityVar    =identity_id,
                              start_dttm_var =,
                              end_dttm_var   =,
                              pageGroupVar   =,
                              outds          =,
                              outdsOpts      =%str(compress=YES)) ;

  %local PageGroups NumPageGroups i group pctPageList pctsecList pctAsecList whereStmt NumPGList 
        PGSecList PGASecList InDsLib InDsName inViya libEngine ;
        
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
    %let whereStmt = %str(&start_dttm_var. <= detail_dttm <= &end_dttm_var.) ;
  %else %if %length(&start_dttm_var.) > 0 %then 
    %let whereStmt = %str(&start_dttm_var. <= detail_dttm) ;
  %else %if %length(&end_dttm_var.) > 0 %then 
    %let whereStmt = %str(detail_dttm <= &end_dttm_var.) ;
  %else %let whereStmt = 1 ;
  
  %if %length(&pageGroupVar.) %then %do ;
    %if (&inViya. = 0) %then %do ;
      proc sql noprint ;
        select distinct &pageGroupVar. into: PageGroups separated by " " from &inds. (where=(&WhereStmt.)) ;
      quit ;
    %end ;
    %else %do ;
      data &InDsLib..PageGroupList_ ;
        set &inds. (keep=&pageGroupVar. &start_dttm_var. detail_dttm &end_dttm_var. where=(&WhereStmt.)) ;
        by &pageGroupVar. ;
        if first.&pageGroupVar. ;
      run ;
      
      proc sql noprint ;
        select &pageGroupVar. into: PageGroups separated by " " from &InDsLib..PageGroupList_ ;
      quit ;
    %end ;
  %end ;

  %let NumPageGroups = %words(&PageGroups) ;

  %if (&inViya. = 0) %then %do ;
    ** Sum to identity and session **;
    proc means data=&inds. (where=(&WhereStmt.)) nway noprint ;
      by &identityVar. ;
      class session_id ;
      var seconds_spent_on_page_cnt active_sec_spent_on_page_cnt ;
      output out=&InDsLib..page_session_ (drop=_type_ rename=(_freq_=NumPages)) sum= ;
    run ;
  
    ** Sum across sessions to identity **;
    proc means data=&InDsLib..page_session_ nway noprint ;
      by &identityVar. ;
      var NumPages seconds_spent_on_page_cnt active_sec_spent_on_page_cnt ;
      output out=&InDsLib..page_session_sum_ (compress=YES drop=_type_ _freq_) 
        sum(NumPages seconds_spent_on_page_cnt active_sec_spent_on_page_cnt)= 
        max(NumPages seconds_spent_on_page_cnt active_sec_spent_on_page_cnt)=MaxNumPagesPerSession MaxSeconds_spent_on_page_PerSess MaxActive_sec_on_page_perSess 
        min(NumPages seconds_spent_on_page_cnt active_sec_spent_on_page_cnt)=MinNumPagesPerSession Minseconds_spent_on_page_PerSess MinActive_sec_on_page_perSess 
        mean(NumPages seconds_spent_on_page_cnt active_sec_spent_on_page_cnt)=AvgNumPagesPerSession Avgseconds_spent_on_page_PerSess AvgActive_sec_on_page_perSess ;
    run ;
  %end ;
  %else %do ;
  
    %CAS_Proc_Means(inds           =&inds.,
                    GroupVars      =&identityVar. session_id,
                    InDsWhere      =%str(&WhereStmt.),
                    Vars           =seconds_spent_on_page_cnt active_sec_spent_on_page_cnt,
                    AggTypeList    =sum n,
                    AggVarNames    =seconds_spent_on_page_cnt active_sec_spent_on_page_cnt NumPages NumPages2,
                    outds          =&InDsLib..page_session_
                    ) ;
                    
    %CAS_Proc_Means(inds           =&InDsLib..page_session_,
                    GroupVars      =&identityVar.,
                    InDsWhere      =,
                    Vars           =NumPages seconds_spent_on_page_cnt active_sec_spent_on_page_cnt,
                    AggTypeList    =sum max min mean,
                    AggVarNames    =NumPages seconds_spent_on_page_cnt active_sec_spent_on_page_cnt 
                                    MaxNumPagesPerSession MaxSeconds_spent_on_page_PerSess MaxActive_sec_on_page_perSess
                                    MinNumPagesPerSession Minseconds_spent_on_page_PerSess MinActive_sec_on_page_perSess
                                    AvgNumPagesPerSession Avgseconds_spent_on_page_PerSess AvgActive_sec_on_page_perSess,
                    outds          =&InDsLib..page_session_sum_
                    ) ;                    

  %end ;
  
  %if (&NumPageGroups. >  0) AND (&inViya. = 0) %then %do ;
    ** Sum to page group level **;
    proc means data=&inds. (where=(&WhereStmt.)) nway noprint ;
      by &identityVar. ;
      class &pageGroupVar. ;
      var seconds_spent_on_page_cnt active_sec_spent_on_page_cnt ;
      output out=&InDsLib..page_grp_ (drop=_type_ rename=(_freq_=NumPages)) sum= ;
    run ;
  
    ** Transpose groups to identity **;
    proc transpose data=&InDsLib..page_grp_ out=&InDsLib..page_grp_T1_ prefix=NumPages_; 
      by &identityVar. ;
      id &pageGroupVar. ;
      var NumPages ;
    run ;
  
    proc transpose data=&InDsLib..page_grp_ out=&InDsLib..page_grp_T2_ prefix=Sec_; 
      by &identityVar. ;
      id &pageGroupVar. ;
      var seconds_spent_on_page_cnt ;
    run ;
  
    proc transpose data=&InDsLib..page_grp_ out=&InDsLib..page_grp_T3_ prefix=ActSec_; 
      by &identityVar. ;
      id &pageGroupVar. ;
      var active_sec_spent_on_page_cnt ;
    run ;
  %end ;
  %else %if (&NumPageGroups. >  0) AND (&inViya. = 1) %then %do ;
  
    %CAS_Proc_Means(inds           =&inds.,
                    GroupVars      =&identityVar. &pageGroupVar.,
                    InDsWhere      =%str(&WhereStmt.),
                    Vars           =seconds_spent_on_page_cnt active_sec_spent_on_page_cnt,
                    AggTypeList    =sum n,
                    AggVarNames    =seconds_spent_on_page_cnt active_sec_spent_on_page_cnt NumPages NumPages2,
                    outds          =&InDsLib..page_grp_
                    ) ;
                    
    proc cas ;
      transpose.transpose / 
        table={
              name="page_grp_",
              caslib="&InDsLib.",
              groupBy={"&identityVar."}
              },
        id={"&pageGroupVar."},
        prefix="NumPages_",
        casOut={name="page_grp_T1_", caslib="&InDsLib.", replace=true},
        transpose={"Numpages"} ;
    quit ;  
    
    proc cas ;
      transpose.transpose / 
        table={
              name="page_grp_",
              caslib="&InDsLib.",
              groupBy={"&identityVar."}
              },
        id={"&pageGroupVar."},
        prefix="Sec_",
        casOut={name="page_grp_T2_", caslib="&InDsLib.", replace=true},
        transpose={"seconds_spent_on_page_cnt"} ;
    quit ;
    
    proc cas ;
      transpose.transpose / 
        table={
              name="page_grp_",
              caslib="&InDsLib.",
              groupBy={"&identityVar."}
              },
        id={"&pageGroupVar."},
        prefix="ActSec_",
        casOut={name="page_grp_T3_", caslib="&InDsLib.", replace=true},
        transpose={"active_sec_spent_on_page_cnt"} ;
    quit ;
  
  %end ;
  
  %do i = 1 %to &NumPageGroups. ;
    %let group = %scan(&PageGroups.,&i.,%str( )) ;
    %let pctPageList = &pctPageList. pctPages_&group. ;
    %let pctSecList  = &pctSecList.  pctSec_&group. ;
    %let pctASecList = &pctASecList. pctASec_&group. ;
    %let NumPGList   = &NumPGList.   NumPages_&group. ;
    %let PGSecList   = &PGSecList.   Sec_&group. ;
    %let PGASecList  = &PGASecList.  ActSec_&group. ;
  %end ;

  data &outds. (&outdsOpts.) ;
    merge &InDsLib..page_session_sum_ (in=insess)
          %if (&NumPageGroups. > 0) %then %do ;
            &InDsLib..page_grp_T1_      (in=in1 drop=_name_) 
            &InDsLib..page_grp_T2_      (in=in2 drop=_name_) 
            &InDsLib..page_grp_T3_      (in=in3 drop=_name_) 
          %end ;;
    by &identityVar. ;
    if not (first.&identityVar. and last.&identityVar.) then abort ;
    
    %if (&NumPageGroups. > 0) %then %do ;
      if not (in1 and in2 and in3) then abort ;
    %end ;
    
    avgSecPerPage = sum(0,seconds_spent_on_page_cnt) / NumPages; 
    avgActiveSecPerPage = sum(0,active_sec_spent_on_page_cnt) / NumPages;

    %if (&NumPageGroups. > 0) %then %do ;
      array numpg    (&NumPageGroups.) &NumPGList. ;
      array secs     (&NumPageGroups.) &PGSecList. ;
      array ActSecs  (&NumPageGroups.) &PGASecList. ;
      array pctpage  (&NumPageGroups.) &pctPageList. ;
      array pctsec   (&NumPageGroups.) &pctSecList.;
      array pctAsec  (&NumPageGroups.) &pctASecList.;
  
      do i = 1 to &NumPageGroups. ;
        if NumPages > 0 AND numPg(i) ne . then pctPage(i) = numPg(i) / NumPages ;
        if seconds_spent_on_page_cnt > 0 AND secs(i) ne . then pctsec(i) = secs(i) / seconds_spent_on_page_cnt ;
        if active_sec_spent_on_page_cnt > 0 AND ActSecs(i) ne . then pctAsec(i) = ActSecs(i) / active_sec_spent_on_page_cnt ;
      end ;
    %end ;
    
    array nums (*) _numeric_ ;
    do i = 1 to dim(nums) ;
      if nums(i) = . then nums(i) = 0 ;
    end ;
    
    drop i ;
    
    label NumPages                         = "Total Num Pages Viewed"
          avgSecPerPage                    = "Avg Seconds per Page"
          avgActiveSecPerPage              = "Avg Active Seconds per Page"
          seconds_spent_on_page_cnt        = "Total Seconds Spent Viewing Pages" 
          active_sec_spent_on_page_cnt     = "Total Active Seconds Spent Viewing pages" 
          MaxNumPagesPerSession            = "Max Num Pages per Session"
          MaxSeconds_spent_on_page_PerSess = "Max Seconds spent per Session" 
          MaxActive_sec_on_page_perSess    = "Max Active Seconds spent per Session"
          MinNumPagesPerSession            = "Min Num Pages per Session"
          MinSeconds_spent_on_page_PerSess = "Min Seconds spent per Session" 
          MinActive_sec_on_page_perSess    = "Min Active Seconds spent per Session"
          AvgNumPagesPerSession            = "Avg Num Pages per Session"
          AvgSeconds_spent_on_page_PerSess = "Avg Seconds spent per Session" 
          AvgActive_sec_on_page_perSess    = "Avg Active Seconds spent per Session"
          %do i = 1 %to &NumPageGroups. ;
            %let group = %scan(&PageGroups.,&i.,%str( )) ;
            NumPages_&group. = "Num Pages viewed in group=&group."
            Sec_&group.      = "Num seconds viewing in group=&group."
            ActSec_&group.   = "Num Active seconds viewing in group=&group."
            pctPages_&group. = "% Pages viewed in group=&group." 
            pctSec_&group.   = "% seconds viewing in group=&group."
            pctASec_&group.  = "% active seconds viewing in group=&group."
          %end ;
    ;
  run ;
  
  proc datasets library=&InDsLib. nolist ;
    delete page_session_ page_session_sum_ page_grp_ page_grp_T1_ page_grp_T2_ page_grp_T3_ PageGroupList_ ;
  quit ;
  
%mend ;
