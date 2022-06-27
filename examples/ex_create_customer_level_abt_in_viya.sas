/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

%StopWatch(start,Banner=**** Start Processing ABT data ***) ;

************************************************************;
**                 Setup Parameters                       **;
************************************************************;

%let ABT_start_dttm = '28FEB2021 00:00:00'dt ;
%let ABT_end_dttm   = '12MAY2021 00:00:00'dt ;
%let path = /mydata ;

cas mycas ;

caslib disc datasource=(srctype=PATH) path="&path." ;
libname disc CAS caslib=disc;
libname discover "&path." ;

******************************************************************;
** Create ABT Customer Universe with Response Flags and amounts **;
******************************************************************;

** load data with conversion/responders and other IDs to be included in ABT **;
proc casutil incaslib="disc" outcaslib="disc";
  load casdata="ABT_universe.sas7bdat" casout="ABT_universe" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;        
quit ;

** clean up data for proc means **; 
data disc.Customer_universe0 ;
  set disc.ABT_universe (keep=identity_id Response_Complete_Flag Response_amt detail_dttm);
  if Response_Complete_Flag = 1 then Responder = 1 ;
  else Responder = 0 ;
  if Response_amt = . or Response_amt <=0 then Response_amt = 0 ;
  rename identity_id=active_identity_id ;
run ;

** Aggregate to the active_identity_id level summing up responder flags and calc earliest response dttm **;
%CAS_Proc_Means(inds           =disc.Customer_universe0,
                GroupVars      =active_identity_id,
                InDsWhere      =,
                Vars           =Responder Donation_amt detail_dttm,
                AggTypeList    =sum min max,
                AggVarNames    =NumResponses Donation_amt drop_me1
                                drop_me2 drop_me3 min_dttm
                                drop_me4 drop_me5 max_dttm,
                outds          =disc.Customer_universe0
                ) ;

** Save identity level customer universe with responders and non-responders **; 
data disc.customer_universe ;
  set disc.customer_universe0 (drop=drop_me1-drop_me5);
  by active_identity_id ;
  if not (first.active_identity_id and last.active_identity_id) then abort ;
  ABT_start_dttm = &ABT_start_dttm. ;
  Responder = (NumResponses >= 1)  ;
  if Responder then ABT_end_dttm = min_dttm ;
  else ABT_end_dttm = &ABT_end_dttm. ;
  format ABT_start_dttm ABT_end_dttm datetime27.6 ;
  keep active_identity_id Responder Response_amt NumResponses ABT_start_dttm ABT_end_dttm ;
  label active_identity_id  = "Identity ID" 
        Responder           = "Response Flag"
  ;
run ; 
 
** clear out memory for other tables **; 
proc casutil incaslib="disc" outcaslib="disc";
  droptable casdata="ABT_universe" ;
  droptable casdata="customer_universe0" ; 
quit ;   

************************************************************;
**                 Promotion data processing              **;
************************************************************;

** Only promotion displayed data is available **; 
proc casutil incaslib="disc" outcaslib="disc";
  load casdata="promotion_displayed.sas7bdat" casout="promotion_displayed" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
  *load casdata="promotion_used.sas7bdat" casout="promotion_used" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
quit ;

** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
data disc.promotion_displayed ;
  merge disc.promotion_displayed (in=inmain)
        disc.customer_universe   (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
  ;
  by active_identity_id ;
  if inuniv and inmain ;
  
  if channel_nm <= "" then channel_nm = "other" ;
run ;

%Make_Promotion_Identity_Lvl(promoDispInDs  =disc.promotion_displayed,
                             promoUsedInDs  =,
                             identityVar    =active_identity_id,
                             start_dttm_var =ABT_start_dttm,
                             end_dttm_var   =ABT_end_dttm,
                             GroupVar       =channel_nm,
                             outds          =disc.promotions_id
                             ) ;                                                        

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

** add page grouping variable in CAS so its easy to change this an re-run **;
data disc.page_details_all ;
  set disc.page_details_all ; 
  length pageGroup $30. ;

  if index(lowcase(page_desc),"page not found") > 0 then pageGroup = "other" ;
  else if domain_nm = "www.home.com" and url_tail = ""  then pageGroup = "home" ;
  else if urllevel1 = "stories" then pageGroup = "stories" ;
  else if urllevel1 = "search" then pageGroup = "search" ;
  else if urllevel1 = "initiatives" then pageGroup = "initiatives" ;
  else if urllevel1 = "places" then pageGroup = "places" ;
  else if urllevel1 = "media" then pageGroup = "media" ;
  else if urllevel1 = "about" then pageGroup = "about" ;
  else if urllevel1 = "magazine" then pageGroup = "magazine" ;
  else if urllevel1 = "blogs" then pageGroup = "blogs" ;
  else if urllevel1 = "industries" then pageGroup = "industries" ;
  else if urllevel1 = "videos" then pageGroup = "videos" ;
  else if urllevel1 = "photos" then pageGroup = "photos" ;
  else pageGroup = "other" ;
 
  if pageGroup <= "" then abort ;
run ;

** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
data disc.page_details_all ;
  merge disc.page_details_all  (in=inmain)
        disc.customer_universe (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
  ;
  by active_identity_id ;
  if inuniv and inmain ;
run ;

%Make_Page_Identity_Lvl(inds           =disc.page_details_all,
                        identityVar    =active_identity_id,
                        start_dttm_var =ABT_start_dttm,
                        end_dttm_var   =ABT_end_dttm,
                        pageGroupVar   =pageGroup,
                        outds          =disc.page_details_all_id,
                        outdsOpts      =) ;
                        
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

data disc.visit_details ;
  set disc.visit_details ;
  length VisitGrp $26. ;
  
  if lowcase(origination_nm) = "google" OR index(lowcase(referrer_domain_nm),"google") then VisitGrp = "Google" ;
  else if lowcase(origination_nm) = "bookmark" then VisitGrp = "Bookmark" ;
  else if lowcase(origination_nm) = "paid search" then VisitGrp = "PaidSearch" ;
  else if lowcase(origination_nm) = "facebook" OR index(lowcase(referrer_domain_nm),"facebook") then VisitGrp = "Facebook" ;
  else if lowcase(origination_nm) = "unlisted campaign" then VisitGrp = "UnlistedCamp" ; 
  else if lowcase(origination_nm) = "verizon" then VisitGrp = "Verizon" ;
  else VisitGrp = "other" ;
  if VisitGrp <= "" then abort ;
run ;

** merge in the start and end dates for the ABT and limit to ABT universe of IDs **;
data disc.visit_details ;
  merge disc.visit_details     (in=inmain)
        disc.customer_universe (in=inuniv keep=active_identity_id ABT_start_dttm ABT_end_dttm)
  ;
  by active_identity_id ;
  if inuniv and inmain ;
run ;

%Make_Visit_Identity_Lvl(inds           =disc.visit_details ,
                         identityVar    =active_identity_id,
                         start_dttm_var =ABT_start_dttm,
                         end_dttm_var   =ABT_end_dttm,
                         VisitGroupVar  =VisitGrp,
                         outds          =disc.visit_details_id,
                         outdsOpts      =) ;

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
  merge disc.customer_universe   (in=inuniv keep=active_identity_id Responder NumResponses Response_amt)
        disc.promotions_id       (in=inpromo) 
        disc.page_details_all_id (in=inpage)
        disc.session_details_id  (in=insession)
        disc.visit_details_id    (in=invisit)
  ;
  by active_identity_id ;
  if not (first.active_identity_id and last.active_identity_id) then abort ;
  if inuniv ;
run ;

proc casutil incaslib="disc" outcaslib="disc"; 
  droptable casdata="customer_universe" ;
  droptable casdata="promotions_id" ;
  droptable casdata="page_details_all_id" ;
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