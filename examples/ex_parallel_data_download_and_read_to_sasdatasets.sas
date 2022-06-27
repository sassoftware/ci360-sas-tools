/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

/*   
     Step 0 Set up folder structure for Throttle_UDM_Data_Download macro.  Under the outfolder should be:
          - gzip_files folder to hold the raw downloaded data files
          - iteration_files folder to hold some temp files that can be useful for debugging if issues arise
          - run_jobs folder to hold the sub programs that get run in parallel and thier logs
          - Also, autoexec file and the include file should be set up 
     Step 1 Download data with parallel streams by time periods using CI360 API 
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
libname out_run "&outFolder./run_jobs" ;

%let DSC_AUTH_TOKEN = ;
%Gen_JWT(tenant_id         = %str(<insert tenant_id>),
         secret_key        = %str(<insert secret_key>),
         method            = datastep,
         out_macrovar_name = DSC_AUTH_TOKEN) ;
         
%Throttle_UDM_Data_Download(JWT                     =%superq(DSC_AUTH_TOKEN),
                            mart_name               =detail,                             
                            ExtGatewayAddress       =%str(https://extapigwservice-prod.ci360.sas.com/marketingGateway),
                            testModeParms           =%nrstr(&schemaVersion=7&category=DISCOVER&includeAllHourStatus=true),                            
                            limit                   =10,
                            dataRangeStartTime      ='14MAR2021 00:00:00'dt,
                            dataRangeEndTime        ='12MAY2021 00:00:00'dt,
                            ParallelMethod          =systask,
                            tables                  =, 
                            NumDaysPerCall          =0.2,
                            MaxNumParallelJobs      =20,
                            StartJob                =1,
                            raw_data_path           =%str(&gzfileFolder.),
                            IncludeFile             =&outFolder./run_jobs/Download_Data_IncludeFile.sas,
                            RunFolder               =&outFolder./run_jobs, 
                            OutIterFilePath         =%str(&outFolder./iteration_files),
                            AutoexecFile            =/u/nopowe/autoexec2.sas,
                            OutJobSummaryDs         =out_run._jobs_,
                            OutCheckLogDs           =out_run.Check_Log,
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