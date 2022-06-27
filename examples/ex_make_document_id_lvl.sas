/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

%let ABT_start_dttm = '23NOV2021 00:00:00'dt ;
%let ABT_end_dttm   = '24JAN2022 00:00:00'dt ;

** load data with conversion/responders and other IDs to be included in ABT **;
proc casutil incaslib="disc" outcaslib="disc";
  load casdata="document_details.sas7bdat" casout="document_details" replace 
    importoptions=(filetype="basesas" dataTransferMode="parallel")  ;        
quit ;

data disc.document_details ;
  set disc.document_details ;
  length group $24. ;
  ABT_start_dttm = &ABT_start_dttm. ;
  ABT_end_dttm = &ABT_end_dttm. ;
  group = compress(scan(uri_txt,1,"/"),"-~.") ;
  format ABT_start_dttm ABT_end_dttm datetime27.6 ;
  label active_identity_id  = "Identity ID"         
  ;
run ;   

%Make_Document_Identity_Lvl(inds           =disc.document_details,
                            identityVar    =active_identity_id,
                            start_dttm_var =ABT_start_dttm,
                            end_dttm_var   =ABT_end_dttm,
                            GroupVar       =group,
                            outds          =disc.testing,
                            outdsOpts      =%str(compress=YES)) ;
