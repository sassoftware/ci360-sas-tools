/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

%StopWatch(start,Banner=**** Start Processing ABT data ***) ;

************************************************************;
**                 Setup Parameters                       **;
************************************************************;
** Analysis period dates **;
%let ABT_start_dttm = '23NOV2021 00:00:00'dt ;
%let ABT_end_dttm   = '24JAN2022 00:00:00'dt ;
%let task_id = xyz ;

%let discover_path = /discover ;
%let engage_path   = /engage ;
   
** For viewing SAS datasets if desired **;
libname disc "&discover_path." access=READONLY ;
libname engage "&engage_path." access=READONLY ;

libname abtParms XLSX "/myfiles/Discover_abt_parms.xlsx" ;
%let parmsSheet = parms 

******************************************************************;
**      Create ABT Customer Universe with Response Flag         **;
******************************************************************;

** Suppose we want all visitors impressed by a web task inlcuded in the ABT **;
** we use conversion_milestone data to get the conversions for the task **;

** Get list of customer impression events in the time periods of interest for the task **;
data Impressed ;
  set engage.impression_spot_viewable (keep=active_identity_id task_id impression_viewable_dttm where=(task_id = "&task_id.")) ;
  if &ABT_start_dttm. <= impression_viewable_dttm <= &ABT_end_dttm. ;
run ;

** Get number of impressions, earliest impression dttm and lastest dttm for each customer ID **;
proc sort data=Impressed ; by active_identity_id ; run ;

proc means data=Impressed nway noprint ;
  by active_identity_id ;
  var impression_viewable_dttm ;
  output out=Impressed_uniq (drop=_freq_ _type_) max=_max_ min=_min_ N=_Nobs_ ;
run ;

** Get list of customer conversion events in the time periods of interest for the task **;
data Converted ;
  set engage.conversion_milestone (keep=active_identity_id task_id conversion_milestone_dttm where=(task_id = "&task_id.")) ;
  if &ABT_start_dttm. <= conversion_milestone_dttm <= &ABT_end_dttm. ;
run ;

** Get number of conversions, earliest conversions dttm and lastest dttm for each customer ID **;
proc sort data=Converted ; by active_identity_id ; run ;

proc means data=Converted nway noprint ;
  by active_identity_id ;
  var conversion_milestone_dttm ;
  output out=Converted_uniq (drop=_freq_ _type_) max=_max_ min=_min_ N=_Nobs_ ;
run ;

data Customer_Universe ;
  merge impressed_uniq (in=inImp  keep=active_identity_id _min_ _max_ _nobs_ rename=(_min_=First_Imp_dttm _max_=Last_Imp_dttm _nobs_=NumImp))
        converted_uniq (in=inConv keep=active_identity_id _min_ _max_ _nobs_ rename=(_min_=First_Conv_dttm _max_=Last_Conv_dttm _nobs_=NumConv))
  ;
  by active_identity_id ;
  if not (first.active_identity_id and last.active_identity_id) then abort ;
  if inImp ;
  
  ABT_start_dttm = &ABT_start_dttm. ;
  
  if inConv and Last_Conv_dttm > First_Imp_dttm then do ;
    responder = 1 ;
    ABT_end_dttm = Last_Conv_dttm ;
  end ;
  else do ;
    responder = 0 ;
    ABT_end_dttm = &ABT_end_dttm. ;
  end ;
  
  if NumConv > responder then NumPrevConv = NumConv - 1 ;
  else NumPrevConv = 0 ;
  
  format First_Imp_dttm Last_Imp_dttm First_Conv_dttm Last_Conv_dttm ABT_start_dttm ABT_end_dttm datetime27.6 ;
run ;

** Discover data is based on events with the same event often landing in multiple tables.  **;
** the identity id and timestamp of the conversion event uniquely define it and will be used **;
** to remove the conversion events from the data used to calculate potential driver variables **;
** to predict the conversion event **;

proc sort data=Converted ; by active_identity_id conversion_milestone_dttm ; run ;

data conversion_events ;
  set Converted (keep=active_identity_id conversion_milestone_dttm) ;
  by active_identity_id conversion_milestone_dttm ;
  if first.conversion_milestone_dttm and last.conversion_milestone_dttm ;
run ;

************************************************************;
**                 Session data processing                **;
************************************************************;
              
proc sort sortsize=25G data=disc.session_details_all out=session_details_tmp (compress=YES) ;
  by active_identity_id session_start_dttm ;
run ;

** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
data session_details_tmp ;
  merge session_details_tmp (in=inmain)
        customer_universe   (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
  ;
  by active_identity_id ;
  if inuniv and inmain ;
run ;

** If the conversion event was a new session event, remove these records from the input data **;
data session_details_tmp ;
 merge session_details_tmp  (in=inmain)
       conversion_events  (in=inconv keep=active_identity_id conversion_milestone_dttm rename=(conversion_milestone_dttm=session_start_dttm))
  ;
  by active_identity_id session_start_dttm ;
  if inmain and NOT inconv ;  
run ;
                          
%Make_Session_Identity_Lvl(inds           =session_details_tmp,
                           identityVar    =active_identity_id,
                           start_dttm_var =ABT_start_dttm,
                           end_dttm_var   =ABT_end_dttm,
                           outds          =session_details_id,
                           outdsOpts      =) ;                                                   

*------------------------------------------------------------------------------------*;
*            Create features for all discover tables listed in Spreadsheet           *;
*------------------------------------------------------------------------------------*;

%macro doit() ;

  data abtParms ;
    set abtParms.&parmsSheet. ;
    where ignoreFlag ne 1 ;
    drop ignoreFlag ;
  run ;

  %let varlist = %varlist(abtparms) ;
  %do i = 1 %to %nobs(abtparms) ;
  
    data _null_ ;
      set abtparms (firstobs=&i. obs=&i.) ;
      %do j = 1 %to %words(&varlist.) ;
        %let var = %scan(&varlist.,&j.,%str( )) ;
        call symput("&var.",trim(left(&var.))) ;
      %end ;
    run ;
    
    %let InDsLib  = %scan(&InDs.,1,%str(.)) ;
    %let InDsName = %scan(&InDs.,2,%str(.)) ;
    
    ************************************************************;
    **                 &inds. data processing             **;
    ************************************************************;
    
    proc sort TAGSORT data=&inds. out=tmp (compress=YES) ; by active_identity_id &dttm_var. ; run ;
    
    ** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
    data tmp ;
      merge tmp               (in=inmain)
            customer_universe (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
      ;
      by active_identity_id ;
      if inuniv and inmain ;  
    run ;
    
    ** If the conversion event was a promotion, remove these records from the input data **;
    data tmp ;
     merge tmp (in=inmain)
           conversion_events (in=inconv keep=active_identity_id conversion_milestone_dttm rename=(conversion_milestone_dttm=&dttm_var.))
      ;
      by active_identity_id &dttm_var. ;
      if inmain and NOT inconv ;  
    run ;
    
    %Make_Disc_Detail_Identity_Lvl(inds           =tmp,
                                   identityVar    =&identityVar.,
                                   dttm_var       =&dttm_var.,
                                   ObsCountVar    =&ObsCountVar.,
                                   vars2agg       =&vars2agg.,
                                   NewAggNames    =&NewAggNames.,
                                   NewLabels      =&NewLabels.,
                                   GroupPrefixList=&GroupPrefixList.,
                                   start_dttm_var =&start_dttm_var.,
                                   end_dttm_var   =&end_dttm_var.,
                                   GroupVar       =&GroupVar.,
                                   outds          =&outds.,
                                   outdsOpts      =&outdsOpts.) ;                                          
  
  %end ;
  
  *------------------------------------------------------------------------------------*;
  *                     Merge all to Create Customer Level ABT                         *;
  *------------------------------------------------------------------------------------*;
  
  proc sql noprint ;
    select outds into: outdsList separated by " " from abtParms ;
  quit ;
  
  data Customer_level_ABT (compress=YES) ; 
    merge customer_universe      (in=inuniv )
          session_details_id     (in=insession)    
          %do i = 1 %to %words(&outdsList.) ;
            %let ds = %scan(&outdsList.,&i.,%str( )) ;
            &ds (in=in&i.)
          %end ;        
    ;
    by active_identity_id ;
    if not (first.active_identity_id and last.active_identity_id) then abort ;
    if inuniv ;
    
    array nums (*) _numeric_ ;
    do i = 1 to dim(nums) ;
      if nums(i) = . and upcase(vname(nums(i))) not in ("LAST_CONV_DTTM" "FIRST_CONV_DTTM") then nums(i) = 0 ;
    end ;
    drop i ;
  run ;
  
%mend ;
  
%doit() ;

%StopWatch(stop,Banner=**** Done Processing ABT data ***) ;