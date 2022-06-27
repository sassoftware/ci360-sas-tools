/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

%StopWatch(start,Banner=**** Start Processing data ***) ;

cas mycas sessopts=(timeout=600 metrics=True) ;

caslib rec datasource=(srctype=PATH) path="<abt source>" ;
libname rec CAS caslib=rec;

caslib _ALL_ assign ;

%let addvars = %str(month day browser_nm state_region_cd city_nm postal_cd device_type_nm platform_type_nm 
                   screen_size_txt origination_nm origination_type_nm) ;

proc astore ;
  upload rstore=rec.astore store="<my astore path>" ;
run ;

proc casutil incaslib="rec" outcaslib="rec";
  load casdata="product_views_abt.sas7bdat" casout="product_views_abt" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
  load casdata="custids2score.sas7bdat" casout="custids2score" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
  load casdata="factors.sas7bdat" casout="factors" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;    
quit ;

** Create unique list of viewed products for each identity to exclude these from recommendations **;
data rec.ABT_Unique_Ident_Prod (partition=(identity_id));
  set rec.product_views_abt (keep=Identity_id product_id);
  by Identity_id product_id ;
  if first.product_id ; 
  format _character_ ;
run ;

data rec.product_views_abt (partition=(identity_id)) ;
  set rec.product_views_abt ;
  format _character_ ;
run ;

data rec.custids2score (partition=(identity_id)) ;
  set rec.custids2score ;
  format _character_ ;
run ;

%Make_Product_Recommendations(casSessionName       =mycas,
                              in2ScoreDs           =rec.custids2score,
                              IdentityProductExclDs=rec.ABT_Unique_Ident_Prod,
                              FactorsInds          =rec.factors,
                              astoreInds           =rec.astore, 
                              Identity_id_Var      =identity_id,                        
                              product_id_var       =product_id,
                              DependentVar         =pct_session_sec_on_page,
                              Addl_Predictors      =&addvars.,
                              NumRecsPerIdentity   =5,
                              RecommendationsOutds =rec.recommendations
                             ) ; 

proc cas;
  table.save /
    caslib="rec"
    name="recommendations.sas7bdat"   
    table={name="recommendations", caslib="rec"}
    permission="PUBLICWRITE"
    exportOptions={fileType="BASESAS" compress="YES"}
    replace=True;
quit; 

%StopWatch(stop,Banner=**** Stop Processing data ***) ;

*cas mycas terminate ;

  