/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Make_Session_Identity_Lvl.sas
/ Author    : Noah Powers
/ Created   : 2020
/ Purpose   : This macro creates a customer level summary of the discover session (plus EXT)
/             data. Note the session input data is expected to have the EXT columns included.
/             
/             The variables specified in the VARLIST macro parameter are summarized by
/             outputting the most frequent non-missing value (by customer).
/   
/             In additon, the following metrics are also generated in the output data:
/             - NumSessions 
/             - wtd_pct_active_sec 
/             - MaxDaysBetweenSessions 
/             - MinDaysBetweenSessions 
/             - AvgDaysBetweenSessions 
/             - Sum_active_sec_in_sessn 
/             - Max_active_sec_in_sessn 
/             - Min_active_sec_in_sessn 
/             - Avg_active_sec_in_sessn
/             - Sum_sec_in_sessn 
/             - Max_sec_in_sessn 
/             - Min_sec_in_sessn 
/             - Avg_sec_in_sessn
/
/ FuncOutput: NA
/ Usage     : 
/ Notes     : outrep=LINUX_X86_64
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ inds          The name of the SAS inptut dataset that contains merged session_details
/               and session_details_ext columns in one dataset.
/ Varlist       The list of char variables in the session data to be summarized in the output
/               with the most frequent non-missing value
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
%macro Make_Session_Identity_Lvl(inds           =,
                                 Varlist        =%str(browser_nm country_nm region_nm city_nm organization_nm platform_desc 
                                                user_language_cd screen_color_depth_no screen_size_txt channel_nm device_type_nm 
                                                latitude longitude device_nm platform_type_nm),
                                 identityVar    =identity_id,
                                 start_dttm_var =,
                                 end_dttm_var   =,
                                 outds          =,
                                 outdsOpts      =%str(compress=YES)) ;

  %local numVars i VarLenList MaxNumSessions whereStmt InDsLib InDsName inViya libEngine ;
  
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
   
  %let NumVars = %words(&varlist.) ;
  %do i = 1 %to &NumVars. ;
    %let VarLenList = &VarLenList. %varlen(&inds.,%scan(&varlist.,&i.,%str( ))). ;
  %end ;

  %if %length(&start_dttm_var.) > 0 AND %length(&end_dttm_var.) > 0 %then 
    %let whereStmt = %str(&start_dttm_var. <= session_start_dttm <= &end_dttm_var.) ;
  %else %if %length(&start_dttm_var.) > 0 %then 
    %let whereStmt = %str(&start_dttm_var. <= session_start_dttm) ;
  %else %if %length(&end_dttm_var.) > 0 %then 
    %let whereStmt = %str(session_start_dttm <= &end_dttm_var.) ;
  %else %let whereStmt = 1 ;

  %if (&inViya. = 0) %then %do ;
    proc means data=&inds. (keep=&identityVar. &start_dttm_var. session_start_dttm &end_dttm_var.) nway noprint ;
      by &identityVar. ;
      where &whereStmt. ;
      output out=&InDsLib..SessionCnts_ (drop=_type_ rename=(_freq_=NumSessions)) ;
    run ;
    
    proc sql noprint ;
      select max(NumSessions) into: MaxNumSessions from &InDsLib..SessionCnts_ ;
    quit ;

  %end ;
  %else %do ;
    %CAS_Proc_Means(inds           =&inds.,
                    GroupVars      =&identityVar.,
                    InDsWhere      =%str(&WhereStmt.),
                    Vars           =load_dttm,
                    AggTypeList    =n,
                    AggVarNames    =NumSessions,
                    outds          =&InDsLib..SessionCnts_
                    ) ;
                    
    %CAS_Proc_Means(inds           =&InDsLib..SessionCnts_,
                    GroupVars      =,
                    InDsWhere      =,
                    Vars           =NumSessions,
                    AggTypeList    =max,
                    AggVarNames    =MaxNumSessions,
                    outds          =&InDsLib..SessionCntsmax_
                    ) ;
                    
    proc sql noprint ;
      select MaxNumSessions into: MaxNumSessions from &InDsLib..SessionCntsmax_ ;
    quit ;                    
  %end ;
  
  %let MaxNumSessions = %trim(&MaxNumSessions.) ;
  
  data &outds. (&outdsOpts.) ;
    set &inds. (where=(&whereStmt.)
                rename=(%do i = 1 %to &NumVars. ; %scan(&VarList.,&i.,%str( ))=%scan(&VarList.,&i.,%str( ))0 %end;)) ;
    by &identityVar. ;

    length %do i = 1 %to &NumVars. ; %scan(&VarList.,&i.,%str( )) %scan(&VarLenList.,&i.,%str( )) %end;;

    retain %do i = 1 %to &NumVars. ; v&i.ind %end;;
    retain NumSessions prevSessionStartdt CumSumDays MaxDaysBetweenSessions MinDaysBetweenSessions 
           Sum_active_sec_in_sessn min_active_sec_in_sessn max_active_sec_in_sessn Sum_sec_in_sessn 
           Min_sec_in_sessn Max_sec_in_sessn ;

    %do i = 1 %to &NumVars. ;
      array v&i.val (&MaxNumSessions.) %scan(&VarLenList.,&i.,%str( )) _temporary_ ;
      array v&i.cnt (&MaxNumSessions.) 8. _temporary_ ;
    %end ;

    prevSessionStartdt = lag(session_start_dttm) ;
    if first.&identityVar. then do ;
      NumSessions = 1 ;
      prevSessionStartdt = . ;
      CumSumDays = . ;
      MaxDaysBetweenSessions = . ;
      MinDaysBetweenSessions = . ;

      Sum_active_sec_in_sessn = active_sec_spent_in_sessn_cnt ;
      min_active_sec_in_sessn = active_sec_spent_in_sessn_cnt ;
      max_active_sec_in_sessn = active_sec_spent_in_sessn_cnt ;

      Sum_sec_in_sessn = seconds_spent_in_session_cnt ;
      Min_sec_in_sessn = seconds_spent_in_session_cnt ;
      Max_sec_in_sessn = seconds_spent_in_session_cnt ;
    end ;
    
    else do ;
      NumSessions = Numsessions + 1 ;  
      if prevSessionStartdt ne . then DaysFromLastSession = (session_start_dttm - prevSessionStartdt)/(60*60*24) ;
      if DaysFromLastSession > MaxDaysBetweenSessions then MaxDaysBetweenSessions = DaysFromLastSession ;
      if DaysFromLastSession < MinDaysBetweenSessions OR MinDaysBetweenSessions = . then MinDaysBetweenSessions = DaysFromLastSession ;

      Sum_active_sec_in_sessn = sum(0,Sum_active_sec_in_sessn,active_sec_spent_in_sessn_cnt) ;
      if active_sec_spent_in_sessn_cnt > max_active_sec_in_sessn then max_active_sec_in_sessn = active_sec_spent_in_sessn_cnt ;
      if active_sec_spent_in_sessn_cnt < min_active_sec_in_sessn then min_active_sec_in_sessn = active_sec_spent_in_sessn_cnt ;

      Sum_sec_in_sessn = sum(0,Sum_sec_in_sessn,seconds_spent_in_session_cnt) ;
      if seconds_spent_in_session_cnt > Max_sec_in_sessn then Max_sec_in_sessn = seconds_spent_in_session_cnt ;
      if seconds_spent_in_session_cnt < Min_sec_in_sessn then Min_sec_in_sessn = seconds_spent_in_session_cnt ;

      CumSumDays = sum(0,CumSumDays,DaysFromLastSession) ;
    end ;

    if last.&identityVar. then do ;
      if CumSumDays ne . then AvgDaysBetweenSessions = CumSumDays / (Numsessions - 1) ;
      if Sum_active_sec_in_sessn ne . then Avg_active_sec_in_sessn = Sum_active_sec_in_sessn / NumSessions ;
      if Sum_sec_in_sessn ne . then Avg_sec_in_sessn = Sum_sec_in_sessn / NumSessions ;
      if Sum_sec_in_sessn > 0 then  wtd_pct_active_sec = Sum_active_sec_in_sessn / Sum_sec_in_sessn ;
    end ;

    if first.&identityVar. and last.&identityVar. then do ;
      %do i = 1 %to &NumVars. ; 
        %scan(&VarList.,&i.,%str( )) = %scan(&VarList.,&i.,%str( ))0 ;
      %end;
    end ;
    else do ;

      if first.&identityVar. then do ;
        ** clear values and counts from the arrays and initialize them with values from first by values **;
        %do i = 1 %to &NumVars. ; 
          v&i.val(1) = %scan(&VarList.,&i.,%str( ))0 ;
          v&i.cnt(1) = 1 ;
          v&i.ind = 1;
        %end;
      end ;
      else if last.&identityVar. then do ;
        ** choose the values that appear most frequently to use for single customer record **;
        %do i = 1 %to &NumVars. ;
          v&i.maxcnt = -1 ;
          do i = 1 to v&i.ind ;
            ** largest frequency value across all non-missing values **;
            %if (%index(%scan(&VarLenList.,&i.,%str( )),$)>0) %then %do ;
              %let rhs = " " ;
            %end ;
            %else %let rhs = %str(.) ;
            if (v&i.cnt(i) > v&i.maxcnt) AND (v&i.val(i) NE &rhs.) then v&i.maxcnt = v&i.cnt(i) ;
          end ;
          do i = 1 to v&i.ind ;
            if v&i.cnt(i)=v&i.maxcnt then do ;
              %scan(&VarList.,&i.,%str( )) = v&i.val(i) ;
              i = v&i.ind + 1 ;
            end ;
          end ;
        %end ;
      end ;
      else do ;
        ** look for value in array list, if found increment count value for that item. **;
        ** If not found, add new value to the array and incrment lastval for that array **;
        %do i = 1 %to &NumVars. ;

          v&i.match_found = 0 ;

          do i = 1 to v&i.ind ;
            if %scan(&VarList.,&i.,%str( ))0  = v&i.val(i) then do ;
              v&i.cnt(i) = v&i.cnt(i) + 1 ;
              v&i.match_found = 1 ;
              i = v&i.ind + 1 ;
            end ;
          end ;

          if NOT v&i.match_found then do ;
            v&i.ind = v&i.ind + 1 ;
            v&i.val(v&i.ind) = %scan(&VarList.,&i.,%str( ))0 ;
            v&i.cnt(v&i.ind) = 1 ;
          end ;

        %end ;
      end ;

    end ;

    if last.&identityVar. then output ;
    
    format prevSessionStartdt DATETIME27.6 ;
    keep &identityVar. &varlist. NumSessions wtd_pct_active_sec 
        MaxDaysBetweenSessions MinDaysBetweenSessions AvgDaysBetweenSessions 
        Sum_active_sec_in_sessn Max_active_sec_in_sessn Min_active_sec_in_sessn Avg_active_sec_in_sessn
        Sum_sec_in_sessn Max_sec_in_sessn Min_sec_in_sessn Avg_sec_in_sessn;
    label 
      NumSessions             = "Total Num of Sessions"
      wtd_pct_active_sec      = "% Active Seconds in Session"
      MaxDaysBetweenSessions  = "Max Days between Sessions"
      MinDaysBetweenSessions  = "Min Days between Sessions" 
      AvgDaysBetweenSessions  = "Avg Days between Sessions"
      Sum_active_sec_in_sessn = "Total Active Seconds in Session" 
      Max_active_sec_in_sessn = "Max Active Seconds per session" 
      Min_active_sec_in_sessn = "Min Active Seconds per session" 
      Avg_active_sec_in_sessn = "Avg Active Seconds per session" 
      Sum_sec_in_sessn        = "Total seconds in sessions"
      Max_sec_in_sessn        = "Max Seconds per session" 
      Min_sec_in_sessn        = "Min Seconds per session" 
      Avg_sec_in_sessn        = "Avg Seconds per session"
      %do i = 1 %to &NumVars. ; 
        %let var = %scan(&VarList.,&i.,%str( )) ;
        &var. = "Most Freq non-missing &var. value" 
      %end;
    ;
  run ;
  
  proc datasets library=&InDsLib. nolist ;
    delete SessionCnts_ SessionCntsmax_ ;
  quit ;

%mend ;
