/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

/*   
     Step 0 Set up folder structure for Download_UDM_Data_Download macro.  Under the outfolder should be:
          - gzip_files folder to hold the raw downloaded data files
          - iteration_files folder to hold some temp files that can be useful for debugging if issues arise        
     Step 1 Download data in one process using CI360 API 
          - Save log to a file 
          - Capture the total run time with the %stopwatch macro
     Step 2: Read gzip files into SAS datasets per the UDM (e.g. page_details, session_details, etc)
     
     It is recommended that the first time through these steps, that you run just one step and validate 
     that there are no errors before moving on to the next step.
*/

%let outFolder = /mnt/bigdisk/discover_data ;
%let gzfileFolder = &outFolder./gzip_files ;

/* STEP 1 BEGIN */
 
** Save the log to a file in the outFolder **;
proc printto log="&outfolder./Download_data.log" NEW ;
run ;

%StopWatch(start,Banner=**** Start Download Discover detail data ***) ;

libname out "&outFolder." ;

%let DSC_AUTH_TOKEN = ;
%Gen_JWT(tenant_id         = %str(<insert tenant_id>),
         secret_key        = %str(<insert secret_key>),
         method            = datastep,
         out_macrovar_name = DSC_AUTH_TOKEN) ;
         
%Download_UDM_Data(JWT                     =%superq(DSC_AUTH_TOKEN),
                   mart_name               =detail,
                   Tables                  =,
                   ExtGatewayAddress       =%str(extapigwservice-prod.ci360.sas.com),     
                   schemaVersion           =9,
                   category                =DISCOVER,
                   addlAPIParms            =%nrstr(&includeAllHourStatus=true),
                   raw_data_path           =%str(&gzfileFolder.),                   
                   limit                   =10,
                   dataRangeStartTimeStamp =2022-04-30T00:00:00.000Z,
                   dataRangeEndTimeStamp   =2022-06-27T00:00:00.000Z,                   
                   OutFiles2ReadNm         =Files2Read,
                   OutTimeStampsNm         =TimeStamps,
                   OutSchemaNm             =Schema,
                   OutIterFilePath         =%str(&outFolder./iteration_files),
                   outlib                  =out) ;

%StopWatch(stop,Banner=**** Done Download Discover detail data ***) ;

proc printto ;
run ;

/* STEP 1 END */

/* STEP 2 BEGIN */

proc printto log="&outfolder./Read_data.log" NEW ;
run ;

%StopWatch(start,Banner=**** Start Read data ***) ;

** Read the *.gz files in raw_data_path into SAS datasets in Outlib **;
%Read_UDM_Data(Files2ReadDs      =out.Files2Read,
               SchemaDs          =out.Schema,
               FilesUncompressed =N,
               raw_data_path     =%str(&gzfileFolder.),
               UncompressCommand =%str(gzip -cd),
               outlib            =out) ;

%StopWatch(stop,Banner=**** Done Read data ***) ;

proc printto ;
run ;

/* STEP 2 END */