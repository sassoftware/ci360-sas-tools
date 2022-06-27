/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

cas mycas ;

libname ident "/identity_metadata_folder" ;

caslib disc datasource=(srctype=PATH) path="/discover_data_folder" ;
libname disc CAS caslib=disc;

caslib _ALL_ assign ;

proc casutil incaslib="disc" outcaslib="disc";
  load casdata="product_views.sas7bdat" casout="product_views" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
  load casdata="session_details_all.sas7bdat" casout="session_details_all" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
  load casdata="page_details_all.sas7bdat" casout="page_details_all" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;    
  load casdata="visit_details.sas7bdat" casout="visit_details" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;      
quit ;

** Get the event ID that goes with the name from CI360 UI **;
data prod_view_event_md ;
  set ident.md_event ;
  if event_nm = "<event name from ci360 UI>" ;
  call symput("event_designed_id",strip(event_id)) ;
run ;

** Get list of unique product IDs (or product_sku IDs) **;
proc freqtab data=disc.product_views (where=(event_designed_id="&event_designed_id.")) missing ;
  tables product_id / out=disc.product_id_list ;
run ;

** (optional) remove certain products from ABT **;
data disc.master_product_list ;
  set disc.product_id_list ;
  if count > 500 ;
run ;

%Make_ProdView_abt(EventID               =&event_designed_id.,
                   Identity_id_Var       =active_identity_id,
                   product_views_ds      =disc.product_views,
                   product_id_var        =product_id,                   
                   product_white_list_ds =disc.master_product_list,
                   page_details_ds       =disc.page_details_all,
                   session_details_ds    =disc.session_details_all,
                   visit_details_ds      =disc.visit_details,                   
                   outABT_ds             =disc.product_views,
                   out2Scoreonly_ds      =disc.custids2score) ; 

** Save the resulting CAS memory tables to SAS datasets **;
proc cas;
  table.save /
    caslib="disc"
    name="product_views_abt.sas7bdat"   
    table={name="product_views", caslib="disc"}
    permission="PUBLICWRITE"
    exportOptions={fileType="BASESAS" compress="YES"}
    replace=True;

  table.save /
    caslib="disc"
    name="custids2score.sas7bdat"   
    table={name="custids2score2", caslib="disc"}
    permission="PUBLICWRITE"
    exportOptions={fileType="BASESAS" compress="YES"}
    replace=True;
quit;

cas mycas terminate ;