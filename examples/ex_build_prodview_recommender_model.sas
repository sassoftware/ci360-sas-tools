/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
*/

cas mazster sessopts=(timeout=600 metrics=True) ;

caslib rec datasource=(srctype="path") path="/abt_source_data_location" ;
libname rec CAS caslib="rec" ;

caslib _ALL_ assign ;

%let addvarsfull = %str(month day browser_nm state_region_cd city_nm postal_cd 
                        device_type_nm platform_type_nm screen_size_txt 
                        origination_nm origination_type_nm) ;

proc casutil incaslib="rec" outcaslib="rec";
  load casdata="product_views_abt.sas7bdat" casout="product_views_abt" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;         
quit ;

%build_prodview_recommender_model(inABT_ds             =rec.product_views_abt,
                                  Identity_id_Var      =identity_id,                        
                                  product_id_var       =product_id,
                                  DependentVar         =seconds_spent_on_page_cnt,
                                  Addl_Predictors      =&addvarsfull.,
                                  ValidationPctObs     =0.3,
                                  nFactors             =,
                                  maxIter              =,
                                  LearnStep            =, 
                                  FactorsOutds         =rec.factors,
                                  ScoredAbtOutds       =rec.scored_abt,
                                  astoreOutds          =rec.astore                                                          
                                 ) ; 

proc cas;
  table.save /
    caslib="rec"
    name="factors.sas7bdat"   
    table={name="factors", caslib="rec"}
    permission="PUBLICWRITE"
    exportOptions={fileType="BASESAS" compress="YES"}
    replace=True;
quit; 

proc astore ;
  download rstore=rec.astore store="<astore_save_path>" ;
run ;

*cas mazster terminate ;