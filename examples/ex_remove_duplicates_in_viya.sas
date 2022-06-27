/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

/*   
     Step 0 Set up dups sub-folder to hold any duplicate records removed from main datasets
     Step 1 Edit and run this code to remove any duplicate records found in the data
*/

%let outFolder = /mnt/bigdisk/discover_data ;

%StopWatch(start,Banner=**** Start Remove Duplicates ***) ;

cas mazter ;

libname sasdat "&outFolder." ;
 
caslib source datasource=(srctype=PATH) path="&outFolder." ;
libname source CAS caslib=source;

caslib dups datasource=(srctype=PATH) path="&outFolder./dups" ;
libname dups CAS caslib="dups" ;

proc casutil incaslib=source ;
  load casdata="timestamps.sas7bdat" casout="timestamps" outcaslib=source;
run ;

%Remove_UDM_Data_Dups(inlib             =source,
                      SortInputData     =N,
                      CompressOutput    =Y,                 
                      duplib            =dups,
                      TimeStampsDs      =sasdat.timestamps,
                      MakeIdentityIDIndx=N,
                      SortVarsXL        =/u/nopowe/CI360_SAS_Tools/discover_table_sort_vars.xlsx,
                      SortVarsXLSheet   =mysheet,                         
                      PctDupsErr        =%str(0.01)
                      ) ;

%StopWatch(stop,Banner=**** Done Removing Duplicates ***) ;

cas mazter terminate ;        