/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

** Start CAS session **; 
cas yourock ;

caslib discover datasource=(srctype=PATH ) path="/my/discover_data" ;
libname disc CAS caslib="discover" ;
libname disc_sd "/my/discover_data" ;

** load session and page details and thier ext tables into CAS **;
proc casutil incaslib="discover" outcaslib="discover";
  load casdata="page_details.sas7bdat" casout="page_details" replace 
       importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
  load casdata="page_details_ext.sas7bdat" casout="page_details_ext" replace 
       importoptions=(filetype="basesas" dataTransferMode="parallel")  ;       
  load casdata="session_details.sas7bdat" casout="session_details" replace 
       importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
  load casdata="session_details_ext.sas7bdat" casout="session_details_ext" replace 
       importoptions=(filetype="basesas" dataTransferMode="parallel")  ;              
run ;

** Use the CAS libname statement for inlib and outlib so data processing done in CAS **;
%Combine_Base_and_Ext_Disc_Detail(inlib        =disc,
                                  TimeStampsDs =disc_sd.timestamps,
                                  names        =%str(page session),
                                  sortData     =N,
                                  outlib       =disc) ;

** Save the CAS tables to sasdatasets **;
proc cas;
  table.save /
    caslib="discover"
    name="page_details_all.sas7bdat"
    table={name="page_details_all", caslib="discover"}
    permission="PUBLICWRITE"
    exportOptions={fileType="BASESAS" compress="YES"}
    replace=True;
  table.save /
    caslib="discover"
    name="session_details_all.sas7bdat"
    table={name="session_details_all", caslib="discover"}
    permission="PUBLICWRITE"
    exportOptions={fileType="BASESAS" compress="YES"}
    replace=True;
quit;                                  

proc cas ;                                                                    
  table.save /
    caslib="discover"
    name="page_details_ext_only.sas7bdat"
    table={name="page_details_ext_only", caslib="discover"}
    permission="PUBLICWRITE"
    exportOptions={fileType="BASESAS" compress="YES"}
    replace=True;
  table.save /
    caslib="discover"
    name="session_details_ext_only.sas7bdat"
    table={name="session_details_ext_only", caslib="discover"}
    permission="PUBLICWRITE"
    exportOptions={fileType="BASESAS" compress="YES"}
    replace=True;                                                                    
quit ;       
                                                             
cas yourock terminate ;                                  