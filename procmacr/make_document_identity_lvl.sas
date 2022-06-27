/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Make_Document_Identity_Lvl.sas
/ Author    : Noah Powers
/ Created   : 2022
/ Purpose   : This macro creates a customer level summary of the discover document_details
/             data. 
/             
/             The following metrics are also generated in the output data:
/             Totals across all documents:
/             - NumDocsViewed 
/
/             Avg value across all sessions:
/             - AvgNumDocsViewedPerSession 
/             - MinNumDocsViewedPerSession 
/             - MaxNumDocsViewedPerSession 
/
/             For each unique value of GROUPVAR:
/             - NumDocs_ (Num docs viewed in group)
/             - PctDocs_ (Pct in group docs viewed)
/
/ FuncOutput: NA
/ Usage     : 
/ Notes     : The values of the groupVar should NOT contain spaces.  This code creates
/             a space delimited list of the unique groupVar values.
/
/             NJP 4.19.22 Perhaps adding group metrics within session would be valuable 
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
%macro Make_Document_Identity_Lvl(inds           =,
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
    %let whereStmt = %str(&start_dttm_var. <= link_event_dttm <= &end_dttm_var.) ;
  %else %if %length(&start_dttm_var.) > 0 %then 
    %let whereStmt = %str(&start_dttm_var. <= link_event_dttm) ;
  %else %if %length(&end_dttm_var.) > 0 %then 
    %let whereStmt = %str(link_event_dttm <= &end_dttm_var.) ;
  %else %let whereStmt = 1 ;
  
  %if %length(&GroupVar.) %then %do ;
    %if (&inViya. = 0) %then %do ;
      proc sql noprint ;
        select distinct &GroupVar. into: Groups separated by " " from &inds. (where=(&WhereStmt.)) ;
      quit ;
    %end ;
    %else %do ;
      data &InDsLib..GroupList_ ;
        set &inds. (keep=&GroupVar. &start_dttm_var. link_event_dttm &end_dttm_var. where=(&WhereStmt.)) ;
        by &GroupVar. ;
        if first.&GroupVar. ;
      run ;
      
      proc sql noprint ;
        select &GroupVar. into: Groups separated by " " from &InDsLib..GroupList_ ;
      quit ;
    %end ;
  %end ;
  
  %let NumGroups = %words(&Groups.) ;

  %if (&inViya. = 0) %then %do ;
    ** Sum to identity and session **;
    proc means data=&inds. (where=(&WhereStmt.)) nway noprint ;
      by &identityVar. ;
      class session_id ;
      var load_dttm ;
      output out=&InDsLib..doc_session_ (drop=_type_ _freq_) N=NumDocs ;
    run ;
  
    ** Sum across sessions to identity **;
    proc means data=&InDsLib..doc_session_ nway noprint ;
      by &identityVar. ;
      var NumDocs ;
      output out=&InDsLib..doc_session_sum_ (compress=YES drop=_type_ _freq_)         
        sum(NumDocs) =NumDocs 
        mean(NumDocs)=AvgNumDocsPerSession 
        min(NumDocs) =MinNumDocsPerSession 
        max(NumDocs) =MaxNumDocsPerSession ;
    run ;
  %end ;
  %else %do ;
  
    %CAS_Proc_Means(inds           =&inds.,
                    GroupVars      =&identityVar. session_id,
                    InDsWhere      =%str(&WhereStmt.),
                    Vars           =load_dttm,
                    AggTypeList    =n,
                    AggVarNames    =NumDocs,
                    outds          =&InDsLib..doc_session_
                    ) ;
                    
    %CAS_Proc_Means(inds           =&InDsLib..doc_session_,
                    GroupVars      =&identityVar.,
                    InDsWhere      =,
                    Vars           =NumDocs,
                    AggTypeList    =sum mean min max,
                    AggVarNames    =NumDocs 
                                    AvgNumDocsPerSession
                                    MinNumDocsPerSession
                                    MaxNumDocsPerSession,
                    outds          =&InDsLib..doc_session_sum_
                    ) ;                    

  %end ;
  
  %if (&NumGroups. >  0) AND (&inViya. = 0) %then %do ;
    ** Sum to group level **;
    proc means data=&inds. (where=(&WhereStmt.)) nway noprint ;
      by &identityVar. ;
      class &GroupVar. ;
      var load_dttm ;
      output out=&InDsLib..doc_grp_ (drop=_type_ _freq_) N=NumDocs ;
    run ;
  
    ** Transpose groups to identity **;
    proc transpose data=&InDsLib..doc_grp_ out=&InDsLib..doc_grp_T1_ prefix=NumDocs_; 
      by &identityVar. ;
      id &GroupVar. ;
      var NumDocs ;
    run ;
  
  %end ;
  %else %if (&NumGroups. >  0) AND (&inViya. = 1) %then %do ;
  
    %CAS_Proc_Means(inds           =&inds.,
                    GroupVars      =&identityVar. &GroupVar.,
                    InDsWhere      =%str(&WhereStmt.),
                    Vars           =load_dttm,
                    AggTypeList    =N,
                    AggVarNames    =NumDocs,
                    outds          =&InDsLib..doc_grp_
                    ) ;
                    
    proc cas ;
      transpose.transpose / 
        table={
              name="doc_grp_",
              caslib="&InDsLib.",
              groupBy={"&identityVar."}
              },
        id={"&GroupVar."},
        prefix="NumDocs_",
        casOut={name="doc_grp_T1_", caslib="&InDsLib.", replace=true},
        transpose={"NumDocs"} ;
    quit ;  
  
  %end ;
  
  %do i = 1 %to &NumGroups. ;
    %let group   = %scan(&Groups.,&i.,%str( )) ;
    %let pctList = &pctList. pctDocs_&group. ;   
    %let NumList = &NumList. NumDocs_&group. ;
  %end ;

  data &outds. (&outdsOpts.) ;
    merge &InDsLib..doc_session_sum_ (in=insess)
          %if (&NumGroups. > 0) %then %do ;
            &InDsLib..doc_grp_T1_    (in=in1 drop=_name_)             
          %end ;;
    by &identityVar. ;
    if not (first.&identityVar. and last.&identityVar.) then abort ;
    
    %if (&NumGroups. > 0) %then %do ;
      if not (in1) then abort ;
    %end ;
    
    %if (&NumGroups. > 0) %then %do ;
      array numdoc  (&NumGroups.) &NumList. ;      
      array pctdoc  (&NumGroups.) &pctList. ;    
  
      do i = 1 to &NumGroups. ;
        if NumDocs > 0 AND numDoc(i) ne . then pctDoc(i) = numDoc(i) / NumDocs ;
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
        NumDocs_&group. comma10.           
        pctDocs_&group. percent12.2 
      %end ;
    ;
    
    label NumDocs                         = "Total Documents Clicked"          
          AvgNumDocsPerSession            = "Avg Num Docs Clicked per Session"         
          MinNumDocsPerSession            = "Min Num Docs Clicked per Session"         
          MaxNumDocsPerSession            = "Max Num Docs Clicked per Session"         
          %do i = 1 %to &NumGroups. ;
            %let group = %scan(&Groups.,&i.,%str( )) ;
            NumDocs_&group. = "Num Docs Clicked in Group=&group."           
            pctDocs_&group. = "% Docs Clicked in Group=&group."            
          %end ;
    ;
  run ;
  
  proc datasets library=&InDsLib. nolist ;
    delete GroupList_ doc_session_ doc_session_sum_ doc_grp_ doc_grp_T1_ ;
  quit ;
  
  proc datasets library=WORK nolist ;
    delete _conts_ ;
  quit ;

%mend ;