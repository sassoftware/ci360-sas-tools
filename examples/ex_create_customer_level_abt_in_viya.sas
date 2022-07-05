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

%let discover_path = /mydata/discover ;
%let engage_path   = /mydata/engage ;

cas mycas ;

caslib disc datasource=(srctype=PATH) path="&discover_path." ;
libname disc CAS caslib=disc;

caslib engage datasource=(srctype=PATH) path="&engage_path." ;
libname engage CAS caslib=engage;

** For viewing SAS datasets if desired **;
libname discover "&discover_path." ;
libname engagdat "&engage_path." ;

******************************************************************;
**      Create ABT Customer Universe with Response Flag         **;
******************************************************************;

** Suppose we want all visitors impressed by a web task inlcuded in the ABT **;
** we use conversion_milestone data to get the conversions for the task **;

** load needed data **;
proc casutil incaslib="engage" outcaslib="engage";
  load casdata="conversion_milestone.sas7bdat" casout="conversion_milestone" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;        
  load casdata="impression_spot_viewable.sas7bdat" casout="impression_spot_viewable" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;        
quit ;

** Get list of customer impression events in the time periods of interest for the task **;
data engage.Impressed ;
  set engage.impression_spot_viewable (keep=active_identity_id task_id impression_viewable_dttm where=(task_id = "&task_id.")) ;
  if &ABT_start_dttm. <= impression_viewable_dttm <= &ABT_end_dttm. ;
run ;

** Get number of impressions, earliest impression dttm and lastest dttm for each customer ID **;
proc cas;
  session mycas ;
  simple.summary /                               
    inputs={"impression_viewable_dttm"},                         
    subSet={"MAX", "MIN", "N"},        
    table={
       caslib="engage",
       name="Impressed",
       groupBy={"active_identity_id"}},
    casout={caslib="engage", name="Impressed_uniq", replace=True}; 
run;

** Get list of customer conversion events in the time periods of interest for the task **;
data engage.Converted ;
  set engage.conversion_milestone (keep=active_identity_id task_id conversion_milestone_dttm where=(task_id = "&task_id.")) ;
  if &ABT_start_dttm. <= conversion_milestone_dttm <= &ABT_end_dttm. ;
run ;

** Get number of conversions, earliest conversions dttm and lastest dttm for each customer ID **;
proc cas;
  session mycas ;
  simple.summary /                               
    inputs={"conversion_milestone_dttm"},                         
    subSet={"MAX", "MIN", "N"},        
    table={
       caslib="engage",
       name="Converted",
       groupBy={"active_identity_id"}},
    casout={caslib="engage", name="Converted_uniq", replace=True}; 
run;

data disc.Customer_Universe ;
  merge engage.impressed_uniq (in=inImp  keep=active_identity_id _min_ _max_ _nobs_ rename=(_min_=First_Imp_dttm _max_=Last_Imp_dttm _nobs_=NumImp))
        engage.converted_uniq (in=inConv keep=active_identity_id _min_ _max_ _nobs_ rename=(_min_=First_Conv_dttm _max_=Last_Conv_dttm _nobs_=NumConv))
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

data engage.conversion_events ;
  set engage.Converted (keep=active_identity_id conversion_milestone_dttm) ;
  by active_identity_id conversion_milestone_dttm ;
  if first.conversion_milestone_dttm and last.conversion_milestone_dttm ;
run ;

** clear out memory for other tables **; 
proc casutil incaslib="engage" outcaslib="engage";
  droptable casdata="conversion_milestone" ;
  droptable casdata="impression_spot_viewable" ; 
quit ;   

************************************************************;
**                 Promotion data processing             **;
************************************************************;

** load discover data physical files into memory/cas **;
proc casutil incaslib="disc" outcaslib="disc";
  load casdata="promotion_displayed.sas7bdat" casout="promotion_displayed" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
quit ;

** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
data disc.promotion_displayed ;
  merge disc.promotion_displayed (in=inmain)
        disc.customer_universe   (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
  ;
  by active_identity_id ;
  if inuniv and inmain ;  
run ;

** If the conversion event was a promotion, remove these records from the input data **;
data disc.promotion_displayed ;
 merge disc.promotion_displayed (in=inmain)
       engage.conversion_events (in=inconv keep=active_identity_id conversion_milestone_dttm rename=(conversion_milestone_dttm=display_dttm))
  ;
  by active_identity_id display_dttm ;
  if inmain and NOT inconv ;  
run ;

%Make_Disc_Detail_Identity_Lvl(inds           =disc.promotion_displayed,
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
                               outds          =disc.promotion_displayed_id,
                               outdsOpts      =%str(compress=YES)) ;
                   
** Drop the discover tables from memory **;
proc casutil incaslib="disc" outcaslib="disc";
  droptable casdata="promotion_displayed" ;
quit ;                            

************************************************************;
**                 Page data processing                   **;
************************************************************;

** load page_details (all) into CAS **;
proc casutil incaslib="disc" outcaslib="disc";  
  load casdata="page_details_all.sas7bdat" casout="page_details_all" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel") ; 
quit ; 

** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
data disc.page_details_all ;
  merge disc.page_details_all  (in=inmain)
        disc.customer_universe (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
  ;
  by active_identity_id ;
  if inuniv and inmain ;
run ;

** If the conversion event was a page view, remove these records from the input data **;
data disc.page_details_all ;
 merge disc.page_details_all    (in=inmain)
       engage.conversion_events (in=inconv keep=active_identity_id conversion_milestone_dttm rename=(conversion_milestone_dttm=detail_dttm))
  ;
  by active_identity_id detail_dttm ;
  if inmain and NOT inconv ;  
run ;

** Create the page data predictor variables **;
%Make_Disc_Detail_Identity_Lvl(inds           =disc.page_details_all,
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
                               outds          =disc.page_details_id,
                               outdsOpts      =%str(compress=YES)) ;

** table no longer needed so remove from memory **;                        
proc casutil incaslib="disc" outcaslib="disc";
  droptable casdata="page_details_all" ; 
quit ;  

************************************************************;
**                 Visit data processing                  **;
************************************************************;

proc casutil incaslib="disc" outcaslib="disc";
  load casdata="visit_details.sas7bdat" casout="Visit_details" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel") ;
quit ;

** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
data disc.visit_details ;
  merge disc.visit_details     (in=inmain)
        disc.customer_universe (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
  ;
  by active_identity_id ;
  if inuniv and inmain ;
run ;

** If the conversion event was a new visit event, remove these records from the input data **;
data disc.visit_details ;
 merge disc.visit_details       (in=inmain)
       engage.conversion_events (in=inconv keep=active_identity_id conversion_milestone_dttm rename=(conversion_milestone_dttm=visit_dttm))
  ;
  by active_identity_id visit_dttm ;
  if inmain and NOT inconv ;  
run ;
                         
%Make_Disc_Detail_Identity_Lvl(inds           =disc.visit_details,
                               identityVar    =active_identity_id,
                               dttm_var       =visit_dttm,
                               ObsCountVar    =NumVisits,
                               vars2agg       =,
                               NewAggNames    =,
                               NewLabels      =Num Visits,
                               GroupPrefixList=,
                               start_dttm_var =ABT_start_dttm,
                               end_dttm_var   =ABT_end_dttm,
                               GroupVar       =,
                               outds          =disc.visit_details_id,
                               outdsOpts      =%str(compress=YES)) ;                         

proc casutil incaslib="disc" outcaslib="disc";
  droptable casdata="visit_details" ;
quit ;  

************************************************************;
**                 Session data processing                **;
************************************************************;

proc casutil incaslib="disc" outcaslib="disc";
  load casdata="session_details_all.sas7bdat" casout="session_details_all" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel") ;
quit ;

** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
data disc.session_details_all ;
  merge disc.session_details_all (in=inmain)
        disc.customer_universe   (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
  ;
  by active_identity_id ;
  if inuniv and inmain ;
run ;

** If the conversion event was a new session event, remove these records from the input data **;
data disc.session_details_all ;
 merge disc.session_details_all  (in=inmain)
       engage.conversion_events  (in=inconv keep=active_identity_id conversion_milestone_dttm rename=(conversion_milestone_dttm=session_start_dttm))
  ;
  by active_identity_id session_start_dttm ;
  if inmain and NOT inconv ;  
run ;
                          
%Make_Session_Identity_Lvl(inds           =disc.session_details_all,
                           identityVar    =active_identity_id,
                           start_dttm_var =ABT_start_dttm,
                           end_dttm_var   =ABT_end_dttm,
                           outds          =disc.session_details_id,
                           outdsOpts      =) ;                         
                           
proc casutil incaslib="disc" outcaslib="disc";
  droptable casdata="session_details_all" ; 
quit ;                           

*------------------------------------------------------------------------------------*;
*                     Merge all to Create Customer Level ABT                         *;
*------------------------------------------------------------------------------------*;

data disc.Customer_level_ABT ; 
  merge disc.customer_universe      (in=inuniv )
        disc.promotion_displayed_id (in=inpromo) 
        disc.page_details_id        (in=inpage)
        disc.session_details_id     (in=insession)
        disc.visit_details_id       (in=invisit)
  ;
  by active_identity_id ;
  if not (first.active_identity_id and last.active_identity_id) then abort ;
  if inuniv ;
run ;

proc casutil incaslib="disc" outcaslib="disc"; 
  droptable casdata="customer_universe" ;
  droptable casdata="promotion_displayed_id" ;
  droptable casdata="page_details_id" ;
  droptable casdata="session_details_id" ;
  droptable casdata="visit_details_id" ;
quit ;  

proc cas;
  table.save /
    caslib="disc"
    name="customer_level_abt.sas7bdat"   
    table={name="customer_level_abt", caslib="disc"}
    permission="PUBLICWRITE"
    exportOptions={fileType="BASESAS" compress="YES"}
    replace=True;
quit;   

%StopWatch(stop,Banner=**** Done Processing ABT data ***) ;

*cas mycas terminate ;