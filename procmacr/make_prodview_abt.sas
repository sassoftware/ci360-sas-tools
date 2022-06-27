/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Make_ProdView_abt.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2021 June
/ LastModBy : Noah Powers
/ LastModDt : 06.28.2021
/ Purpose   : Create a transactional style ABT dataset with all product views for a given 
/             user specified product view event that also includes session and visit data 
/             user specified columns.  This is desgined to be used with proc factmac. 
/
/             This macro will also create a dataset for scoring those identities that 
/             did not have a product view in the ABT. 
/            
/             Diagnostic Data Created:
/             - &outlib..nosess To capture any product view records without corresponding Session data
/             - &outlib..bouncers To capture any bounce sessions that are dropped from ABT
/             - &outlib..dropped_products To capture any records dropped from ABT because 
/               product was not on the white list ds
/ FuncOutput: N/A
/ Usage     :
/ Notes     : It is bettter for factorization machines model to have transactional style data 
/             with multiple product views if available. So no need to aggregate multiple product 
/             views within or across sessions - but this can be done if desired.
/
/             Dependent variable idea: For those identities with multiple sessions, 
/             create index variable that compares recent seconds on the page/act sec on page/etc 
/             to avg values across sessions in the time period of interest.  The intent is
/             to find unusually high/low values compared to average over time within a 
/             identity.  The challenge is that most users dont have many sessions - maybe in this
/             case use an across identity average?
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name      Description
/ -------------------------------------------------------------------------------------
/ EventID               The event_designed_id for the product view event of interest.  Only
/                       records with this event_designed_id will be included in the ABT
/ Identity_id_Var       The column in the Discover tables that captures the customer ID.  This
/                       is typically identity_id or active_identity_id.
/ product_views_ds      The name of the dataset that contains the product_views Discover data
/ product_id_var        Which product identifier to use.  This should be either (product_id or 
/                       product_sku)
/ Extra_ProdView_Vars   additional prod view cols to include (opt)
/ product_white_list_ds (optional) A dataset that contains a list of product IDs that are 
/                       to be included in the output ABT dataset
/ page_details_ds       The name of the page details discover data 
/ session_details_ds    The name of the session details discover data
/ visit_details_ds      The name of the visit details discover data
/ start_dttm_var        (optional) The earliest/smallest date time value to use in the ABT
/ end_dttm_var          (optional) The lastest/largest date time value to use in the ABT
/ page_vars             (default)  The list of variables from the page details data to include in the ABT
/ session_vars          (default) The list of variables from the session details data to include in the ABT
/ visit_vars            (default) The list of variables from the visit details data to include in the ABT
/ outABT_ds             The name of the output ABT dataset to create
/ out2Scoreonly_ds      The name of the output score only dataset to create
/============================================================================================*/
%macro Make_ProdView_abt(EventID               =,
                         Identity_id_Var       =identity_id,
                         product_views_ds      =,
                         product_id_var        =,
                         Extra_ProdView_Vars   =,
                         product_white_list_ds =,
                         page_details_ds       =,
                         session_details_ds    =,
                         visit_details_ds      =,
                         start_dttm_var        =,
                         end_dttm_var          =,
                         page_vars             =%str(active_sec_spent_on_page_cnt seconds_spent_on_page_cnt),
                         session_vars          =%str(country_cd user_language_cd session_start_dttm last_session_activity_dttm 
                                                     browser_nm state_region_cd city_nm postal_cd device_type_nm platform_type_nm screen_size_txt 
                                                     active_sec_spent_in_sessn_cnt seconds_spent_in_session_cnt ),
                         visit_vars            =%str(origination_nm origination_type_nm),
                         outABT_ds             =,
                         out2Scoreonly_ds      =) ; 

  %local outlib OutName pvwhereStmt seswhereStmt ;
  
  ** Extract output library name **;
  %let OutLib  = %scan(&outABT_ds.,1,%str(.)) ;
  %let OutName = %scan(&outABT_ds.,2,%str(.)) ;

  %if not %length(&OutName.) %then %do ;
    %let OutLib  = WORK ;
    %let OutName = &outABT_ds. ;
  %end ;        
  
  ** Create where statement to limit time periods if necessary **;
  %if %length(&start_dttm_var.) > 0 AND %length(&end_dttm_var.) > 0 %then %do ;
    %let pvwhereStmt = %str(&start_dttm_var. <= action_dttm <= &end_dttm_var.) ;
    %let seswhereStmt = %str(&start_dttm_var. <= session_start_dttm <= &end_dttm_var.) ;
  %end ;
  %else %if %length(&start_dttm_var.) > 0 %then %do ;
    %let pvwhereStmt = %str(&start_dttm_var. <= action_dttm) ;
    %let sesWhereStmt = %str(&start_dttm_var. <= session_start_dttm) ;
  %end ;
  %else %if %length(&end_dttm_var.) > 0 %then %do ;
    %let pvwhereStmt = %str(action_dttm <= &end_dttm_var.) ;
    %let seswhereStmt = %str(session_start_dttm <= &end_dttm_var.) ;
  %end ;
  %else %do ;
    %let pvwhereStmt = 1 ;
    %let sesWhereStmt = 1 ;
  %end ;
  
  data &outABT_ds. ;
    set &product_views_ds.   (in=inpv keep=event_designed_id session_id visit_id detail_id &Identity_id_Var. &product_id_var. 
                                               action_dttm &Extra_ProdView_Vars.
                                          where=(event_designed_id = "&EventID." AND &pvwhereStmt.)) ;
    length month day $2. ;
    if action_dttm ne . then do ;
      month = put(month(datepart(action_dttm)),2.) ;
      day   = put(day(datepart(action_dttm)),2.) ;
    end ;
    keep session_id visit_id detail_id &Identity_id_Var. &product_id_var. action_dttm &Extra_ProdView_Vars. month day ; 
  run ;
  
  ** take most recent session for scoring records **;
  data &out2Scoreonly_ds.  (partition=(session_id)) ;
    set &session_details_ds. (in=insess keep=session_id &Identity_id_Var. session_start_dttm &session_vars.
                                where=(&sesWhereStmt.)) ;
    by &Identity_id_Var. DESCENDING session_start_dttm ;
    length month day $2. ;
    
    if first.session_start_dttm ;
    if session_start_dttm ne . then do ;
      month = put(month(datepart(session_start_dttm)),2.) ;
      day   = put(day(datepart(session_start_dttm)),2.) ;
    end ;
  run ;
   
  %if %length(&session_vars.) %then %do ;
    ** Merge in session predictors to the product view data **;
    ** Also Get all sessions for scoring post modeling **;
    
    data &outABT_ds.         
         &outlib..nosess ; 
      merge &outABT_ds.          (in=inabt)
            &session_details_ds. (in=insess keep=session_id &session_vars. seconds_spent_in_session_cnt active_sec_spent_in_sessn_cnt)  
      ;
      by session_id ;
      
      if inabt ;
      if not insess then output &outlib..nosess ;
     
      output &outABT_ds. ;
    run ;
  
  %end ;
  
  %if %length(&visit_vars.) %then %do ;
  
    ** Merge in columns from visit_details data **;
    data &outABT_ds. ;
      merge &outABT_ds.        (in=inabt)
            &visit_details_ds. (in=invisit keep=visit_id &visit_vars.) 
      ;
      by visit_id ;
      if inabt ;
      *if not invisit then abort ; 
    run ;
  
    ** Add visit detail measures to scoring records **;  
    data &out2Scoreonly_ds. (partition=(&Identity_id_Var.)); 
      merge &out2Scoreonly_ds. (in=inbase)
            &visit_details_ds. (in=invisit keep=session_id visit_dttm &visit_vars.)
      ; 
      by session_id ;
      if inbase ;
      if not invisit then abort ;
    run ;
    
    ** Can be multiple visits within a session - take most recent one **;
    data &out2Scoreonly_ds. (partition=(&Identity_id_Var.)); 
      set &out2Scoreonly_ds. ; 
      by &Identity_id_Var. descending visit_dttm ;
      if first.&Identity_id_Var. ;
    run ;
  
  %end ;
    
  %if %length(&page_vars.) %then %do ;
      
    ** merge in data from page_details data **;
    data &outABT_ds.
         &outlib..bouncers ;
      merge &outABT_ds.       (in=inabt)
            &page_details_ds. (in=inpage keep=detail_id &page_vars. seconds_spent_on_page_cnt active_sec_spent_on_page_cnt)   
      ;
      by detail_id ;
      if inabt ;
      if not inpage then abort ; 
      if seconds_spent_on_page_cnt < 1 then output &outlib..bouncers ;
      
      if active_sec_spent_in_sessn_cnt > 0 then pct_session_active_sec_on_page = active_sec_spent_on_page_cnt / active_sec_spent_in_sessn_cnt ;
        else pct_session_active_sec_on_page = 0 ;
      if seconds_spent_in_session_cnt > 0 then pct_session_sec_on_page = seconds_spent_on_page_cnt / seconds_spent_in_session_cnt ;
        else pct_session_sec_on_page = 0 ;
        
      if pct_session_active_sec_on_page < 0 then pct_session_active_sec_on_page = 0 ;
      if pct_session_active_sec_on_page > 1 then pct_session_active_sec_on_page = 1 ;
      if pct_session_sec_on_page < 0 then pct_session_sec_on_page = 0 ;
      if pct_session_sec_on_page > 1 then pct_session_sec_on_page = 1 ;
      
      output &outABT_ds. ;
      
      format pct_session_active_sec_on_page pct_session_sec_on_page percent6.1 ;
    run ;
  
  %end ;
  
  %if %length(&product_white_list_ds.) %then %do ;
  
    ** remove products not in the white list **;
    data &outABT_ds.
         &outlib..dropped_products ;
      merge &outABT_ds.             (in=inview)
            &product_white_list_ds. (in=inprods keep=&product_id_var.) 
      ;
      by &product_id_var. ;
      if inview AND inprods then output &outABT_ds. ;
      else output &outlib..dropped_products ;
    run ;
    
  %end ;
   
%mend ;