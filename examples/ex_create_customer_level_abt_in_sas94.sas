/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

%StopWatch(start,Banner=**** Start Processing ABT data ***) ;

************************************************************;
**                 Setup Parameters                       **;
************************************************************;
** Analysis period dates **;
%let ABT_start_dttm = '01JUL2022 00:00:00'dt ;
%let ABT_end_dttm   = '10JUL2022 00:00:00'dt ;

%let task_id = xyz ;

%let discover_path = /mydata/discover ;
%let engage_path   = /mydata/engage ;
   
libname disc   "&discover_path." ;
libname engage "&engage_path." ;

******************************************************************;
**      Create ABT Customer Universe with Response Flag         **;
******************************************************************;

** Suppose we want all visitors impressed by a web task inlcuded in the ABT **;
** we use conversion_milestone data to get the conversions for the task **;

** Get list of customer impression events in the time periods of interest for the task **;
data Impressed (compress=YES) ;
  set engage.impression_spot_viewable (keep=active_identity_id task_id impression_viewable_dttm where=(task_id = "&task_id.")) ;
  if &ABT_start_dttm. <= impression_viewable_dttm <= &ABT_end_dttm. ;
run ;

proc sort data=Impressed ; by active_identity_id ; run ;

** Get number of impressions, earliest impression dttm and lastest dttm for each customer ID **;
proc means data=Impressed nway noprint ;
  by active_identity_id ;
  var impression_viewable_dttm ;
  output out=Impressed_Uniq (drop=_freq_ _type_) max=_max_ min=_min_ N=_Nobs_ ;
run ;

** Get list of customer conversion events in the time periods of interest for the task **;
data Converted ;
  set engage.conversion_milestone (keep=active_identity_id task_id conversion_milestone_dttm where=(task_id = "&task_id.")) ;
  if &ABT_start_dttm. <= conversion_milestone_dttm <= &ABT_end_dttm. ;
run ;

proc sort data=Converted ; by active_identity_id conversion_milestone_dttm ; run ;

** Get number of conversions, earliest conversions dttm and lastest dttm for each customer ID **;
proc means data=Converted nway noprint ;
  by active_identity_id ;
  var conversion_milestone_dttm ;
  output out=Converted_Uniq (drop=_freq_ _type_) max=_max_ min=_min_ N=_Nobs_ ;
run ;

data Customer_Universe (compress=YES) ;
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

data conversion_events ;
  set Converted (keep=active_identity_id conversion_milestone_dttm) ;
  by active_identity_id conversion_milestone_dttm ;
  if first.conversion_milestone_dttm and last.conversion_milestone_dttm ;
run ;

************************************************************;
**                 Promotion data processing             **;
************************************************************;
proc sort data=disc.promotion_displayed ; by active_identity_id ; run ;

** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
data promotion_displayed (compress=YES) ;
  merge disc.promotion_displayed (in=inmain)
        customer_universe        (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
  ;
  by active_identity_id ;
  if inuniv and inmain ;  
run ;

proc sort data=promotion_displayed ; by active_identity_id display_dttm ; run ;

** If the conversion event was a promotion, remove these records from the input data **;
data promotion_displayed (compress=YES) ;
 merge promotion_displayed (in=inmain)
       conversion_events   (in=inconv keep=active_identity_id conversion_milestone_dttm rename=(conversion_milestone_dttm=display_dttm))
  ;
  by active_identity_id display_dttm ;
  if inmain and NOT inconv ;  
run ;

%Make_Disc_Detail_Identity_Lvl(inds           =promotion_displayed,
                               identityVar    =active_identity_id,
                               dttm_var       =display_dttm,
                               ObsCountVar    =NumPromoDisp,
                               vars2agg       =,
                               NewAggNames    =,
                               NewLabels      =Num Promotions Displayed,
                               GroupPrefixList=,
                               start_dttm_var =ABT_start_dttm,
                               end_dttm_var   =ABT_end_dttm,
                               GroupVar       =,
                               outds          =promotion_displayed_id,
                               outdsOpts      =%str(compress=YES)) ;                                         

************************************************************;
**                 Page data processing                   **;
************************************************************;
proc sort data=disc.page_details_all ; by active_identity_id ; run ;

** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
data page_details_all (compress=YES) ;
  merge disc.page_details_all  (in=inmain)
        customer_universe      (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
  ;
  by active_identity_id ;
  if inuniv and inmain ;
run ;

proc sort data=page_details_all ; by active_identity_id detail_dttm ; run ;

** If the conversion event was a page view, remove these records from the input data **;
data page_details_all (compress=YES) ;
 merge page_details_all  (in=inmain)
       conversion_events (in=inconv keep=active_identity_id conversion_milestone_dttm rename=(conversion_milestone_dttm=detail_dttm))
  ;
  by active_identity_id detail_dttm ;
  if inmain and NOT inconv ;  
run ;

** Create the page data predictor variables **;
%Make_Disc_Detail_Identity_Lvl(inds           =page_details_all,
                               identityVar    =active_identity_id,
                               dttm_var       =detail_dttm,
                               ObsCountVar    =NumPages,
                               vars2agg       =seconds_spent_on_page_cnt active_sec_spent_on_page_cnt,
                               NewAggNames    =sec_on_page act_sec_on_page,
                               NewLabels      =Num Pages Viewed |Num seconds spent on page|Num active seconds spent on page,
                               GroupPrefixList=,
                               start_dttm_var =ABT_start_dttm,
                               end_dttm_var   =ABT_end_dttm,
                               GroupVar       =,
                               outds          =page_details_id,
                               outdsOpts      =%str(compress=YES)) ;

data page_details_id (compress=YES) ;
  set page_details_id ;
  avgSecPerPage = sum(0,sec_on_page) / NumPages; 
  avgActiveSecPerPage = sum(0,act_sec_on_page) / NumPages;
  
  label avgSecPerPage                    = "Avg Seconds per Page"
        avgActiveSecPerPage              = "Avg Active Seconds per Page" ;
run ;

************************************************************;
**                 Visit data processing                  **;
************************************************************;

proc sort data=disc.visit_details  ; by active_identity_id ; run ;

** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
data visit_details (compress=YES) ;
  merge disc.visit_details (in=inmain)
        customer_universe  (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
  ;
  by active_identity_id ;
  if inuniv and inmain ;
run ;

proc sort data=visit_details  ; by active_identity_id visit_dttm ; run ;

** If the conversion event was a new visit event, remove these records from the input data **;
data visit_details (compress=YES);
 merge visit_details     (in=inmain)
       conversion_events (in=inconv keep=active_identity_id conversion_milestone_dttm rename=(conversion_milestone_dttm=visit_dttm))
  ;
  by active_identity_id visit_dttm ;
  if inmain and NOT inconv ;  
  
  ** Example of using time based buckets for group variable **;
  ** Could group into week starting sunday buckets  **; 
  week_beg = intnx('week', datepart(visit_dttm), 0, 'beginning');
  ** Could group into bi-weekly starting sunday buckets  **;
  biweek_beg = intnx('week2', datepart(visit_dttm), 0, 'beginning');
  group_var = put(biweek_beg,date8.) ;
  format week_beg biweek_beg date8. ;
run ;
                         
%Make_Disc_Detail_Identity_Lvl(inds           =visit_details,
                               identityVar    =active_identity_id,
                               dttm_var       =visit_dttm,
                               ObsCountVar    =NumVisits,
                               vars2agg       =,
                               NewAggNames    =,
                               NewLabels      =Num Visits,
                               GroupPrefixList=NumVisits,
                               start_dttm_var =ABT_start_dttm,
                               end_dttm_var   =ABT_end_dttm,
                               GroupVar       =group_var,
                               outds          =visit_details_id,
                               outdsOpts      =%str(compress=YES)) ;                         

************************************************************;
**                 Session data processing                **;
************************************************************;

proc sort data=disc.session_details_all  ; by active_identity_id ; run ;

** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
data session_details_all (compress=YES) ;
  merge disc.session_details_all (in=inmain)
        customer_universe        (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
  ;
  by active_identity_id ;
  if inuniv and inmain ;
run ;

proc sort data=session_details_all  ; by active_identity_id session_start_dttm ; run ;

** If the conversion event was a new session event, remove these records from the input data **;
data session_details_all (compress=YES) ;
 merge session_details_all  (in=inmain)
       conversion_events    (in=inconv keep=active_identity_id conversion_milestone_dttm rename=(conversion_milestone_dttm=session_start_dttm))
  ;
  by active_identity_id session_start_dttm ;
  if inmain and NOT inconv ;  
run ;
                          
%Make_Session_Identity_Lvl(inds           =session_details_all,
                           identityVar    =active_identity_id,
                           start_dttm_var =ABT_start_dttm,
                           end_dttm_var   =ABT_end_dttm,
                           outds          =session_details_id,
                           outdsOpts      =) ;                         
                           
*------------------------------------------------------------------------------------*;
*                     Merge all to Create Customer Level ABT                         *;
*------------------------------------------------------------------------------------*;

data Customer_level_ABT ; 
  merge customer_universe      (in=inuniv )
        promotion_displayed_id (in=inpromo) 
        page_details_id        (in=inpage)
        session_details_id     (in=insession)
        visit_details_id       (in=invisit)
  ;
  by active_identity_id ;
  if not (first.active_identity_id and last.active_identity_id) then abort ;
  if inuniv ;
run ; 

%StopWatch(stop,Banner=**** Done Processing ABT data ***) ;