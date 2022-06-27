/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

libname outhub "D:\Temp" ;

%let DSC_AUTH_TOKEN = ;
%Gen_JWT(tenant_id         = %str(<insert tenant_id>),
         secret_key        = %str(<insert secret_key>),
         method            = datastep,
         out_macrovar_name = DSC_AUTH_TOKEN) ;
         
** Get full list of data files that have been uploaded to the HUB **;
%get_360hub_file_metadata(JWT      =%superq(DSC_AUTH_TOKEN),
                          url_base =%nrstr(https://extapigwservice-demo.cidemo.sas.com),
                          limit    =1000,
                          outTableListds=outhub.CI360HubFileList,
                          outVarListDs  =outhub.CI360HubVarList) ;

data outhub.HubFiles2Download ;
  set outhub.CI360HubFileList ;
  if _N_ <= 50 ;
run ;

** Download the *.gz files associated with the TableListds **;
** There is a known feature that limits each user to 50 POST calls (e.g. 50 files to download) per day **;
** If the tableLisdDs has more than 50 files you MUST reduce this to < 50 or the macro will FAIL **;
** This is a CI360 API limitation **;  
%Download_360_HubFiles(JWT      =%superq(DSC_AUTH_TOKEN),
                       url_base =%nrstr(https://extapigwservice-demo.cidemo.sas.com),                     
                       TableListds=outhub.HubFiles2Download,
                       raw_data_path=%str(D:\Temp\raw Hub Data),
                       OutHubFileList=Outhub.Hubfiles2Read
                       ) ;

** This macro reads the *.gz files into sas datasets and sets the lenght of char **;
** variables to the smallest len that is needed for the data that was downloaded **;
%Read_360Hub_Data(HubFiles2ReadDs=outhub.HUBFILES2READ,
                  HubTableListDs =outhub.CI360HUBFILELIST,
                  HubVarListDs   =outhub.CI360HUBVARLIST,
                  raw_data_path  =%str(D:\Temp\raw Hub Data),
                  outlib         =outhub) ;
