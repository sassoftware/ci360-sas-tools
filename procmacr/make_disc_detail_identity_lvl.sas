/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Make_Disc_Detail_Identity_Lvl.sas
/ Author    : Noah Powers
/ Created   : 2022
/ Purpose   : This macro creates a customer level summary of the user specified discover detail
/             data (e.g. form_details) 
/             
/             The following metrics are generated in the output data:
/             Totals across all records:
/             - Total Number of Events/obs (obsCountVar)
/             - Totals for all columns in VARS2AGG
/
/             Avg/min/max values across all sessions for each metric above:
/             - Avg<var>PerSess 
/             - Min<var>PerSess 
/             - Max<var>PerSess 
/
/             For each metric above and each unique value of GROUPVAR: 
/             - <var>_<group> (counts in each value of groupvar)
/             - Pct<var>_<group> (percent of var in each value of groupvar)
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
/ inds           The name of the SAS inptut dataset that contains one of the discover detail 
/                tables (e.g. document_details)
/ identityVar    The name of the column to use as the customer ID.  The default is identity_id
/ dttm_var       The name of the date time variable specific to the table 
/ ObsCountVar    Name of the variable to create that will contain the #obs 
/ vars2agg       (optional) List of variables to aggregate in the data.  This does not include a
/                number of records variable as that is automatically handled using N(load_dttm) 
/ NewAggNames    The space delimited list of base names to use for created columns
/ NewLabels      The pipe | delimited list of base labels to use for newly created columns
/ GroupPrefixList If a groupVar is provided, then this must also be provided and should contain
/                 the prefix to use corresponding to the OBSCOUNTVAR and VARS2AGG list with the
/                 group level variables that will be created.
/ start_dttm_var (optional) This is the name of a datetime variable in the input data or a 
/                datetime constant value.  When such a value is provided, only those
/                records with &start_dttm_var. <= session_start_dttm will be included in the 
/                processed data.
/ end_dttm_var   (optional) This is the name of a datetime variable in the input data or a 
/                datetime constant value.  When such a value is provided, only those
/                records with session_start_dttm <= &end_dttm_var. will be included in the 
/                processed data.
/ GroupVar      (optional) The column in the input dataset that is used to group similar
/               documents according to the unique levels of this variable.
/ outds         The name of the output SAS dataset to create at the IDENTITYVAR level
/ outdsOpts     (default compress=YES) optional dataset options to apply to the output 
/               dataset created.
/============================================================================================*/
%macro Make_Disc_Detail_Identity_Lvl(inds           =,
                                     identityVar    =identity_id,
                                     dttm_var       =,
                                     ObsCountVar    =,
                                     vars2agg       =,
                                     NewAggNames    =,
                                     NewLabels      =,
                                     GroupPrefixList=,
                                     start_dttm_var =,
                                     end_dttm_var   =,
                                     GroupVar       =,
                                     outds          =,
                                     outdsOpts      =%str(compress=YES)) ;

  %local InDsLib InDsName inViya libEngine whereStmt NewVars NumNewVars i var avglist minlist maxlist Groups NumGroups pfix
         j group baselabel ;
        
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
    %let whereStmt = %str(&start_dttm_var. <= &dttm_var. <= &end_dttm_var.) ;
  %else %if %length(&start_dttm_var.) > 0 %then 
    %let whereStmt = %str(&start_dttm_var. <= &dttm_var.) ;
  %else %if %length(&end_dttm_var.) > 0 %then 
    %let whereStmt = %str(&dttm_var. <= &end_dttm_var.) ;
  %else %let whereStmt = 1 ;
  
  %let NewVars = &ObsCountVar. &NewAggNames. ;
  %let NumNewVars = %words(&NewVars.) ;
  %do i = 1 %to &NumNewVars. ;
    %let var = %scan(&NewVars.,&i.,%str( )) ;
    %if %length(&var.) > 19 %then %do ;
      %put Error: Variable names must be < 19 chars ;
      %goto FINISH ;
    %end ;
    %let avglist = &avglist. avg_&var._per_sesn ;
    %let minlist = &minlist. min_&var._per_sesn ;
    %let maxlist = &maxlist. max_&var._per_sesn ;
  %end ;
  
  %if %length(&GroupVar.) %then %do ;
    %if (&inViya. = 0) %then %do ;
      proc sql noprint ;
        select distinct &GroupVar. into: Groups separated by " " from &inds. (where=(&WhereStmt.)) ;
      quit ;
    %end ;
    %else %do ;
      data &InDsLib..GroupList_ ;
        set &inds. (keep=&GroupVar. &start_dttm_var. &end_dttm_var. &dttm_var. where=(&WhereStmt.)) ;
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
      var load_dttm &vars2agg. ;
      output out=&InDsLib.._session_ (drop=_type_ _freq_) N(load_dttm)=&ObsCountVar. sum(&vars2agg.)=&NewAggNames. ;
    run ;
  
    ** Sum across sessions to identity **;
    proc means data=&InDsLib.._session_ nway noprint ;
      by &identityVar. ;
      var &ObsCountVar. &NewAggNames. ;
      output out=&InDsLib.._session_sum_ (compress=YES drop=_type_ _freq_)         
        sum(&ObsCountVar. &NewAggNames.)  =
        mean(&ObsCountVar. &NewAggNames.) =&avglist. 
        min(&ObsCountVar. &NewAggNames.)  =&minlist. 
        max(&ObsCountVar. &NewAggNames.)  =&maxlist. ;
    run ;
  %end ;
  %else %do ;
  
    %CAS_Proc_Means(inds           =&inds.,
                    GroupVars      =&identityVar. session_id,
                    InDsWhere      =%str(&WhereStmt.),
                    Vars           =load_dttm &vars2agg.,
                    AggTypeList    =n sum,
                    AggVarNames    =&ObsCountVar. %do i = 1 %to &NumNewVars. ; deleteme&i. %end; &NewAggNames.,
                    outds          =&InDsLib.._session_
                    ) ;
                    
    %CAS_Proc_Means(inds           =&InDsLib.._session_,
                    GroupVars      =&identityVar.,
                    InDsWhere      =,
                    Vars           =&ObsCountVar. &NewAggNames.,
                    AggTypeList    =sum mean min max,
                    AggVarNames    =&ObsCountVar. 
                                    &NewAggNames.
                                    &avglist.
                                    &minlist.
                                    &maxlist.,
                    outds          =&InDsLib.._session_sum_
                    ) ;                    

  %end ;
  
  %if (&NumGroups. >  0) AND (&inViya. = 0) %then %do ;
    ** Sum to group level **;
    proc means data=&inds. (where=(&WhereStmt.)) nway noprint ;
      by &identityVar. ;
      class &GroupVar. ;
      var load_dttm &vars2agg. ;
      output out=&InDsLib.._grp_ (drop=_type_ _freq_) N(load_dttm)=&ObsCountVar. sum(&vars2agg.)=&NewAggNames. ;
    run ;
  
    %do i = 1 %to &NumNewVars. ;
      %let var = %scan(&NewVars.,&i.,%str( )) ;
      %let pfix = %scan(&GroupPrefixList.,&i.,%str( )) ;
      
      ** Transpose groups to identity **;
      proc transpose data=&InDsLib.._grp_ out=&InDsLib.._grp_T&i._ prefix=&pfix._; 
        by &identityVar. ;
        id &GroupVar. ;
        var &var. ;
      run ;
    %end ;
  
  %end ;
  %else %if (&NumGroups. >  0) AND (&inViya. = 1) %then %do ;
  
    %CAS_Proc_Means(inds           =&inds.,
                    GroupVars      =&identityVar. &GroupVar.,
                    InDsWhere      =%str(&WhereStmt.),
                    Vars           =load_dttm &vars2agg.,
                    AggTypeList    =N sum,
                    AggVarNames    =&ObsCountVar. %do i = 1 %to &NumNewVars. ; deleteme&i. %end; &NewAggNames.,
                    outds          =&InDsLib.._grp_
                    ) ;
    %do i = 1 %to &NumNewVars. ;
      %let var = %scan(&NewVars.,&i.,%str( )) ;
      %let pfix = %scan(&GroupPrefixList.,&i.,%str( )) ;
      
      proc cas ;
        transpose.transpose / 
          table={
                name="_grp_",
                caslib="&InDsLib.",
                groupBy={"&identityVar."}
                },
          id={"&GroupVar."},
          prefix="&pfix._",
          casOut={name="_grp_T&i._", caslib="&InDsLib.", replace=true},
          transpose={"&var."} ;
      quit ;  
    %end ;    
  %end ;
  
  %do j = 1 %to %words(&GroupPrefixList.) ;
    %local var&j.list pctvar&j.list ;
  %end ;
  
  %do i = 1 %to &NumGroups. ;
    %let group   = %scan(&Groups.,&i.,%str( )) ;
    %do j = 1 %to %words(&GroupPrefixList.) ;
      %let pfix = %scan(&GroupPrefixList.,&j.,%str( )) ;
      
      %let var&j.list = &&var&j.list. &pfix._&group. ;
      %let pctvar&j.list = &&pctvar&j.list. pct_&pfix._&group. ;      
    %end ;
  %end ;

  data &outds. (&outdsOpts.) ;
    merge &InDsLib.._session_sum_ (in=insess)
          %if (&NumGroups. > 0) %then %do i = 1 %to &NumNewVars. ;
            &InDsLib.._grp_T&i._    (in=in&i. drop=_name_)             
          %end ;;
    by &identityVar. ;
    if not (first.&identityVar. and last.&identityVar.) then abort ;
    
    %if (&NumGroups. > 0) %then %do ;
      if not (1 %do i = 1 %to &NumNewVars. ; and in&i. %end;) then abort ;
    %end ;
    
    %if (&NumGroups. > 0) %then %do ;
      %do j = 1 %to %words(&GroupPrefixList.) ;
        %let pfix = %scan(&GroupPrefixList.,&j.,%str( )) ;      
        array var&j. (&NumGroups.) &&var&j.list. ;
        array pvar&j. (&NumGroups.) &&pctvar&j.list. ;        
      %end ;
       
      do i = 1 to &NumGroups. ;
        %do i = 1 %to &NumNewVars. ;
          %let var = %scan(&NewVars.,&i.,%str( )) ;
          if &var. > 0 AND var&i.(i) ne . then pvar&i.(i) = var&i.(i) / &var. ;          
        %end ;
      end ;
    %end ;
    
    array nums (*) _numeric_ ;
    do i = 1 to dim(nums) ;
      if nums(i) = . then nums(i) = 0 ;
    end ;
    
    drop i ;
    
    label
      %do i = 1 %to &NumNewVars. ;
        %let var = %scan(&NewVars.,&i.,%str( )) ;
        %let pfix = %scan(&GroupPrefixList.,&i.,%str( )) ;
        %let baselabel = %scan(&NewLabels.,&i.,%str(|)) ;
        &var. = "Total &baselabel." 
        avg_&var._per_sesn = "Avg &baselabel. per session" 
        min_&var._per_sesn = "Min &baselabel. per session" 
        max_&var._per_sesn = "Max &baselabel. per session" 
        %do j = 1 %to &NumGroups. ;
          %let group   = %scan(&Groups.,&j.,%str( )) ;
          &pfix._&group. = "&baselabel. for group=&group." 
          pct_&pfix._&group. = "Percent of &baselabel. for group=&group."
        %end ;
      %end ;
    ;    
  run ;
  
  proc datasets library=&InDsLib. nolist ;
    delete GroupList_ _session_ _session_sum_ _grp_ %do i = 1 %to &NumNewVars. ; _grp_T&i._ %end;;
  quit ;

  proc datasets library=WORK nolist ;
    delete _conts_ ;
  quit ;

  %FINISH:
%mend ;