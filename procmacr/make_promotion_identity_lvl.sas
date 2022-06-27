/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Make_Promotion_Identity_Lvl.sas
/ Author    : Noah Powers
/ Created   : 2020
/ Purpose   : This macro creates a customer level summary of the discover Promotion_displayed
/             and promotion_used data. The two datasets are merged together after the
/             promotion_used is aggregated to: session_id visit_id detail_id event_designed_id 
/
/             The following metrics are generated in the output data for each unique 
/             value of the GROUPVAR user supplied parameter: 
/
/             - click&groupVal.  = "Num Clicks for &groupVal."
/             - disp&groupVal.   = "Num displays for &groupVal."
/             - MaxCPS&groupVal. = "Max clicks per session for &groupVal."
/             - MaxDPS&groupVal. = "Max displays per session for &groupVal."
/             - MinCPS&groupVal. = "Min clicks per session for &groupVal."
/             - MinDPS&groupVal. = "Min displays per session for &groupVal."
/             - AvgCPS&groupVal. = "Avg clicks per session for &groupVal."
/             - AvgDPS&groupVal. = "Avg displays per session for &groupVal."
/ FuncOutput: NA
/ Usage     : 
/ Notes     : outrep=LINUX_X86_64
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ promoDispInDs  Name of the input dataset that contains the promotions displayed info
/ promoUsedInDs  Name of the input dataset that contains the promotions used info
/ identityVar    The name of the column to use as the customer ID.  The default is identity_id
/ start_dttm_var (optional) This is the name of a datetime variable in the input data or a 
/                datetime constant value.  When such a value is provided, only those
/                records with &start_dttm_var. <= session_start_dttm will be included in the 
/                processed data.
/ end_dttm_var   (optional) This is the name of a datetime variable in the input data or a 
/                datetime constant value.  When such a value is provided, only those
/                records with session_start_dttm <= &end_dttm_var. will be included in the 
/                processed data.
/ GroupVar      Column in promotion_displayed that relates to the promos/events whose
/               values will be used to create output metrics
/               with the most frequent non-missing value
/ outds         The name of the output SAS dataset to create at the IDENTITYVAR level
/ outdsOpts     (default compress=YES) optional dataset options to apply to the output 
/               dataset created.
/============================================================================================*/
%macro Make_Promotion_Identity_Lvl(promoDispInDs  =,
                                   promoUsedInDs  =,
                                   identityVar    =identity_id,
                                   start_dttm_var =,
                                   end_dttm_var   =,
                                   GroupVar       =,
                                   outds          =,
                                   outdsOpts      =%str(compress=YES)) ;
 
  %local i statement vars2transpose i var whereStmt whereStmt2 DispInDsLib DispInDsName inViya libEngine1
         libEngine2 ;
  
  %let DispInDsLib  = %scan(&promoDispInDs.,1,%str(.)) ;
  %let DispInDsName = %scan(&promoDispInDs.,2,%str(.)) ;

  %if not %length(&DispInDsName.) %then %do ;
    %let DispInDsLib  = WORK ;
    %let DispInDsName = &promoDispInDs. ;
  %end ;        
  
  %let UsedInDsLib  = %scan(&promoUsedInDs.,1,%str(.)) ;
  %let UsedInDsName = %scan(&promoUsedInDs.,2,%str(.)) ;

  %if not %length(&UsedInDsName.) %then %do ;
    %let UsedInDsLib  = WORK ;
    %let UsedInDsName = &promoUsedInDs. ;
  %end ;        
  
  %if ("%substr(&sysvlong.,1,1)" = "V") %then %do ;
    proc contents data=&promoDispInDs. out=_conts1_ noprint ;
    run ;
    
    proc contents data=&promoUsedInDs. out=_conts2_ noprint ;
    run ;
    
    proc sql noprint ;
      select distinct engine into: libEngine1 separated by "*" from _conts1_ ;
      select distinct engine into: libEngine2 separated by "*" from _conts2_ ;
    quit ;
    
    %if ("%upcase(&libEngine1.)" = "CAS") AND ("%upcase(&libEngine2.)" = "CAS") %then %let inViya = 1 ;      
      %else %let inViya = 0 ;
  %end ;
  %else %let inViya = 0 ;
  
  %if %length(&start_dttm_var.) > 0 AND %length(&end_dttm_var.) > 0 %then 
    %let whereStmt = %str(&start_dttm_var. <= click_dttm <= &end_dttm_var.) ;
  %else %if %length(&start_dttm_var.) > 0 %then 
    %let whereStmt = %str(&start_dttm_var. <= click_dttm) ;
  %else %if %length(&end_dttm_var.) > 0 %then 
    %let whereStmt = %str(click_dttm <= &end_dttm_var.) ;
  %else %let whereStmt = 1 ;

  %if %length(&start_dttm_var.) > 0 AND %length(&end_dttm_var.) > 0 %then 
    %let whereStmt2 = %str(&start_dttm_var. <= display_dttm < &end_dttm_var.) ;
  %else %if %length(&start_dttm_var.) > 0 %then 
    %let whereStmt2 = %str(&start_dttm_var. <= display_dttm) ;
  %else %if %length(&end_dttm_var.) > 0 %then 
    %let whereStmt2 = %str(display_dttm < &end_dttm_var.) ;
  %else %let whereStmt2 = 1 ;

  %if %length(&promoUsedInDs.) %then %do ;
    %if (&inViya. = 0) %then %do ;
      proc means data=&promoUsedInDs. (where=(&whereStmt.)) nway noprint ;
        by session_id visit_id detail_id event_designed_id ;
        var click_dttm click_dttm_tz ;
        output out=&DispInDsLib..promo_used_sum_ (drop=_type_ rename=(_freq_=NumClicks)) 
          min(click_dttm click_dttm_tz)= Min_click_dttm Min_click_dttm_tz;
      run ;
    %end ;
    %else %do ;
      %CAS_Proc_Means(inds           =&promoUsedInDs.,
                      GroupVars      =session_id visit_id detail_id event_designed_id,
                      InDsWhere      =%str(&WhereStmt.),
                      Vars           =click_dttm click_dttm_tz,
                      AggTypeList    =min n,
                      AggVarNames    =Min_click_dttm Min_click_dttm_tz NumClicks NumClicks2,
                      outds          =&DispInDsLib..promo_used_sum_
                      ) ;
    %end ;  
  %end ;
  
  %if %length(&promoUsedInDs.) AND %length(&promoDispInDs.) %then %do ;
    data &DispInDsLib..combined         (compress=YES)
         &DispInDsLib..usedNotDisplayed (keep=session_id visit_id detail_id event_designed_id NumClicks Min_click_dttm Min_click_dttm_tz);
      merge &promoDispInDs.               (in=indisp where=(&whereStmt2.))
            &DispInDsLib..promo_used_sum  (in=inused) 
      ;
      by session_id visit_id detail_id event_designed_id ;
      if not (first.EVENT_DESIGNED_ID and last.EVENT_DESIGNED_ID) then abort ;
      if NumClicks = . then NumClicks = 0 ;
      if inused and not indisp then output &DispInDsLib..usedNotDisplayed ;
      else output &DispInDsLib..combined ;
    run ;
  %end ;
  %else %if %length(&promoDispInDs.) %then %do ;
    data &DispInDsLib..combined         (compress=YES) ;
      set &promoDispInDs. (in=indisp where=(&whereStmt2.));
      NumClicks = 0 ;
    run ;
  %end ;

  %if (&inViya. = 0) %then %do ;
    %Tagsort_InMem(inds=&DispInDsLib..combined,
                   outdsOpts=%str(compress=YES),
                   sortbyVars=&identityVar. &GroupVar. session_id,
                   sortOpts=sortsize=25G) ;
  
    ** Sum to identity and session **;
    proc means data=&DispInDsLib..combined nway noprint ;
      by &identityVar. &GroupVar. session_id ;
      var NumClicks ;
      output out=&DispInDsLib..promo_session_ (drop=_type_ rename=(_freq_=NumDisp)) sum= ;
    run ;

    ** Sum across sessions to identity **;
    proc means data=&DispInDsLib..promo_session_ nway noprint ;
      by &identityVar. &GroupVar. ;
      var NumClicks NumDisp ;
      output out=&DispInDsLib..promo_session_sum_ (compress=YES drop=_type_ _freq_) 
        sum(NumClicks NumDisp)=click disp 
        max(NumClicks NumDisp)=MaxCPS MaxDPS 
        min(NumClicks NumDisp)=MinCPS MinDPS  
        mean(NumClicks NumDisp)=AvgCPS AvgDPS  ;
    run ;
  %end ;
  %else %do ;
    %CAS_Proc_Means(inds           =&DispInDsLib..combined,
                    GroupVars      =&identityVar. &GroupVar. session_id,
                    InDsWhere      =,
                    Vars           =NumClicks,
                    AggTypeList    =sum n,
                    AggVarNames    =NumClicks NumDisp,
                    outds          =&DispInDsLib..promo_session_
                    ) ;
                    
    %CAS_Proc_Means(inds           =&DispInDsLib..promo_session_,
                    GroupVars      =&identityVar. &GroupVar.,
                    InDsWhere      =,
                    Vars           =NumClicks NumDisp,
                    AggTypeList    =sum max min mean,
                    AggVarNames    =click disp MaxCPS MaxDPS MinCPS MinDPS AvgCPS AvgDPS,
                    outds          =&DispInDsLib..promo_session_sum_
                    ) ;                    
  %end ;

  %let vars2transpose = click disp MaxCPS MaxDPS MinCPS MinDPS AvgCPS AvgDPS ;

  %let statement = 1 ;
  %do i = 1 %to %words(&vars2transpose.) ;

    %let var = %scan(&vars2transpose.,&i.,%str( )) ;
    %let statement = &statement. and in&i ;
    
    %if (&inViya. = 0) %then %do ;
      proc transpose data=&DispInDsLib..promo_session_sum_ out=&DispInDsLib..cust_promos_T&i. prefix=&var.;
        by &identityVar. ;
        id &GroupVar. ;
        var &var. ;
      run ;
    %end ;
    %else %do ;
      proc cas ;
        transpose.transpose / 
          table={
                name="promo_session_sum_",
                caslib="&DispInDsLib.",
                groupBy={"&identityVar."}
                },
          id={"&GroupVar."},
          prefix="&var.",
          casOut={name="cust_promos_T&i.", caslib="&DispInDsLib.", replace=true},
          transpose={"&var."} ;
      quit ;  
    %end ;
    
  %end ;
  
  %if %length(&GroupVar.) %then %do ;
    %if (&inViya. = 0) %then %do ;
      proc sql noprint ;
        select distinct &GroupVar. into: GroupVarVals separated by " " from &DispInDsLib..promo_session_sum_ ;
      quit ;
    %end ;
    %else %do ;
      data &DispInDsLib..PageGroupList_ ;
        set &promoDispInDs. (keep=&GroupVar.) ;
        by &GroupVar. ;
        if first.&GroupVar. ;
      run ;
      
      proc sql noprint ;
        select &GroupVar. into: GroupVarVals separated by " " from &DispInDsLib..PageGroupList_ ;
      quit ;
    %end ;
  %end ;

  data &outds. (&outdsOpts.) ;
    merge 
      %do i = 1 %to %words(&vars2transpose.) ;
        &DispInDsLib..cust_promos_T&i. (in=in&i. drop=_name_) 
      %end ;
    ;
    by &identityVar. ;
    if not (first.&identityVar. and last.&identityVar.) then abort ;
    if not (&statement.) then abort ;
    array nums (*) _numeric_ ;
    do i = 1 to dim(nums) ;
      if nums(i) = . then nums(i) = 0 ;
    end ;
    drop i ;
    label 
      %do i = 1 %to %words(&GroupVarVals.) ;
        %let groupVal = %scan(&GroupVarVals.,&i.,%str( )) ;
        click&groupVal.  = "Num Clicks for &groupVal."
        disp&groupVal.   = "Num displays for &groupVal."
        MaxCPS&groupVal. = "Max clicks per session for &groupVal."
        MaxDPS&groupVal. = "Max displays per session for &groupVal."
        MinCPS&groupVal. = "Min clicks per session for &groupVal."
        MinDPS&groupVal. = "Min displays per session for &groupVal."
        AvgCPS&groupVal. = "Avg clicks per session for &groupVal."
        AvgDPS&groupVal. = "Avg displays per session for &groupVal."
      %end ;
    ;
  run ;
  
  proc datasets library=&DispInDsLib. nolist ;
    delete promo_used_sum_ combined usedNotDisplayed promo_session_ promo_session_sum_ PageGroupList_
      %do i = 1 %to %words(&vars2transpose.) ; cust_promos_T&i. %end; ;
  quit ;
 
  %FINISH: 
%mend ;