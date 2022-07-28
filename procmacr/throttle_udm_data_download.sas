/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Throttle_UDM_Data_Download.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2020 February
/ LastModBy : Noah Powers
/ LastModDt : 02.15.2020
/ Purpose   : Automate the process of breaking up a request for a large amout of time/data 
/             into many smaller requests for downloading the gzipped Discover data csv files.  The 
/             user chooses between three types of data:
/             - detail (business processes, goals, product views etc)
/             - identity (not time based - three tables)
/             - dbtTables (tables behind the 360 UI reports
/ FuncOutput: N/A
/ Usage     :
/ Notes     :  See the CI360 user manual for full details on how to download the data
/                https://go.documentation.sas.com/?cdcId=cintcdc&cdcVersion=production.a&docsetId=cintag&docsetTarget=extapi-discover-service.htm&locale=en#n1dcw035ykd0xwn1mp0yczqroca6
/
/              To test/see the unified discover/engage tables use these parms:
/              schemaVersion=4
/              code=PH4TESTMODE
/               gives you the unified tables.  
/               You also need to specify the category= parameter. 
/               If you dont you only get the Discover tables (default).
/               Possible categories and tables included are listed here:  
/               http://sww.sas.com/saspedia/CI_360_Subject_Area_Category_and_Licensing 
/
/              If sub-hourly time periods are desired, the TESTMODEPARMS macro paramter
/              can be used with a value such as %nrstr(&subHourlyDataRangeInMinutes=10) to 
/              add this parameter to the API call URL string.
/            
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ JWT                     Java Web Token based on the tenant id and secret key.  
/                         The %Gen_JWT() macro can create this for you.
/ mart_name               This is expected to be one of two time based mart values:
/                         DBTREPORT or DETAIL.  The code will force the correct
/                         case for the API
/ ExtGatewayAddress       The external gateway address for the tenant.  This can be found in the 
/                         CI360 UI at General | External | Access. 
/ limit                   API parameter default value of 20. 
/ testModeParms           List exactly as they should show up on the URL as paramters.  See above
/                         in notes for how to use this macro parameter
/ dataRangeStartTime      Use SAS datetime literal value such as '15DEC2019 00:00:00'dt 
/ dataRangeEndTime        Use SAS datetime literal value such as '14FEB2020 00:00:00'dt
/ Tables                  (optional) If it is desired to limit the tables downloaded, provide
/                         a space delimited list of table names to INCLUDE/DONWLOAD data for. 
/ ParallelMethod          (systask or connect) This is the technology that will be used to run
/                         the macro invocations in parallel.  There are two options: systask OR connect.
/                         The default is systask for backward compatibility. Systask uses the SAS SYSTASK 
/                         command to spawn separate indpendent sas jobs but this requires that XCMD be enabled.
/                         Alternatively, if SAS/Connect is licensed and installed, this option can be 
/                         used instead.
/ ConnectOptions          Optional settings specified using space delimited list of key words.
/                         Currently, only detects NOSASCMD option for situation where SAS is in
/                         a lockdown state prohibits you from specifying the sascmd on the remote
/                         machines.  When the user specifies NOSASCMD for this parameter, the 
/                         sascmd="" option of the SIGNON commannd for SAS/Connect is dropped as 
/                         this not allowed in a lockdown state. A consequence of this is that 
/                         any of the following user parameters are ignored as well:
/                          - ConfigFile 
/                          - AutoexecFile  
/                          - WorkFolder  
/                          - CmdLineOptions 
/                         As these options are all specified on the sascmd statement. 
/ NumDaysPerCall          Default is 0.5. This is used to break the full time period range into
/                         smaller chunks as this works best with the API
/ StartJob                (default = 1).  This parameter can be set to a value > 1 in the event
/                         that a previous run of this macro only partially completed up to job
/                         Startjob -1.
/ RerunJobList            (optional) A spaced delimited list of job numbers to run. If this is 
/                         provided, the StartJob parameter is ignored.  This parameter is 
/                         intended to be used when some of the jobs failed in a prior. 
/                         When this parameter is non-null then the OutJobSummaryDs OutCheckLogDs
/                         datasets are NOT deleted as these are expected to contain records
/                         for prior successfully run jobs 
/ JobTimeoutSec           (default 0 = no timeout)  This parameter is used in the TIMEOUT= 
/                         parameter on the systask or rsubmit statement to cause the job to end 
/                         after JOBTIMOUTSEC seconds even if the job is not complete.
/ MaxNumParallelJobs      (default=5).  This is the number of concurrent API calls, where each 
/                         API handles a chunk of the data as determined by NUMDAYSPERCALL parameter
/ raw_data_path           Path where the raw gzipped files will be stored
/ RunFolder               (default=.) This is the folder where the run parallel jobs code, logs are 
/                         stored
/ AutoexecFile            The full filepath and name to the autoexec.sas file to use with run parallel
/                         jobs macro. 
/ ConfigFile              (optional) The full filepath and name for the SAS config file to use in 
/                         the sub sas jobs
/ IncludeFile             This is the full filepath and name for the inlcude file to be sent to 
/                         run parallel jobs macro.  Typically this holds libnames that all jobs will
/                         need to have.
/ OutJobSummaryDs         (default=_jobs_) This is the dataset name including library where the 
/                         jobs summary dataset from run parallel jobs is stored.
/ OutCheckLogDs           (default=Check_Log) This is the dataset name including library where the 
/                         CheckLog macro summary dataset from run parallel jobs is stored.
/ OutIterFilePath         This is the filepath where the files and datasets for QC purposes from 
/                         each iteration of Download_Discover_Data are stored.
/ Outlib                  The name of the sas library where the list of files to read and schema files 
/                         will be saved.
/============================================================================================*/
%macro Throttle_UDM_Data_Download(JWT                     =,
                                  mart_name               =,                                  
                                  ExtGatewayAddress       =,                                  
                                  limit                   =,
                                  testModeParms           =%nrstr(&schemaVersion=7&category=DISCOVER&includeAllHourStatus=true),
                                  dataRangeStartTime      =,
                                  dataRangeEndTime        =,
                                  Tables                  =,
                                  ParallelMethod          =connect,
                                  ConnectOptions          =,
                                  NumDaysPerCall          =0.5,
                                  StartJob                =1,
                                  RerunJobList            =,
                                  JobTimeoutSec           =1800,
                                  MaxNumParallelJobs      =5,
                                  raw_data_path           =,
                                  RunFolder               =.,
                                  AutoexecFile            =,
                                  ConfigFile              =,
                                  IncludeFile             =,
                                  OutJobSummaryDs         =_jobs_,
                                  OutCheckLogDs           =Check_Log,
                                  OutIterFilePath         =,
                                  outlib                  =WORK
                                  ) ;

  %local NumIter _i_ dt_str Min_Start_dt Max_End_dt files2readList SchemaList schemads TimeStampList schemaVersion ;
  
  %if (not %length(&ParallelMethod.)) %then %do ;
    %let ParallelMethod = connect ;
  %end ;
  %let ParallelMethod = %upcase(&ParallelMethod.) ;

  %if NOT %length(&limit.) %then 
    %let limit = 20 ;

  %if not %length(&StartJob.) %then %let StartJob = 1 ;

  %if "%upcase(&Mart_Name.)" = "IDENTITY" %then %do ;
    %put E%upcase(rror): Use the DOWNLOAD_UDM_DATA macro for the idenity data ;
    %goto FINISH ;
  %end ;

  data _null_ ;
    length dt_str $19. ;
    dt_str = translate(put(datetime(),datetime19.),"_",":") ;
    call symputx("dt_str",strip(dt_str)) ;
  run ;

  data DownloadMacroParms ;
    length dataRangeStartTimeStamp dataRangeEndTimeStamp $30. ;
    length jwt $1000. mart_name $32. Tables $2000. ExtGatewayAddress $1000. raw_data_path $2000. 
           limit 8. testModeParms $500. RenameExistingGZ $1. outlib $32. 
           OutIterFilePath $2000.;
    retain jwt mart_name Tables ExtGatewayAddress raw_data_path 
           limit testModeParms RenameExistingGZ outlib 
           OutIterFilePath ;
 
    JWT                     = '%nrstr(' || "&JWT." || ')' ;
    mart_name               = "&mart_name." ;
    Tables                  = "&Tables." ;
    ExtGatewayAddress       = "&ExtGatewayAddress." ;
    raw_data_path           = "&raw_data_path." ;
    limit                   = &limit. ;  
    testModeParms           = '%nrstr(' || "&testModeParms." || ')' ;
    RenameExistingGZ        = "N" ;
    outlib                  = "&outlib." ;
    OutIterFilePath         = "&OutIterFilePath." ;
  
    iteration = 1 ;
    incr = 60*60*24*&NumDaysPerCall. ;                      

    do dt = &dataRangeStartTime. to &dataRangeEndTime. by incr ;
      start = dt + 1 - (dt = &dataRangeStartTime.) ;
      end = min(&dataRangeEndTime.,dt + incr) ;
      NumDays = (end - start) / (60*60*24) ;

      st_date = put(datepart(start),yymmdd10.) ;
      st_time = put(timepart(start),tod10.) ;
      dataRangeStartTimeStamp = strip(st_date) || "T" || strip(st_time) || ".000Z" ;

      end_date = put(datepart(end),yymmdd10.) ;
      end_time = put(timepart(end),tod10.) ;
      dataRangeEndTimeStamp = strip(end_date) || "T" || strip(end_time) || ".000Z" ;
      if Numdays > 0 then do ;
        FileTag           = "_&dt_str._iter" || strip(put(iteration,z4.)) ;
        OutFiles2ReadNm   = "files2read" || strip(put(iteration,z4.)) ;
        OutTimeStampsNm   = "timestamps" || strip(put(iteration,z4.)) ;
        OutSchemaNm       = "schema" || strip(put(iteration,z4.)) ;          
        output ;
        iteration = iteration + 1 ;
      end ;
    end ;
    format start end dt datetime20. ;
    drop st_date st_time end_date end_time dt ;
  run ;

  %if %eval(&StartJob. > %nobs(DownloadMacroParms)) %then %do ;
    %put %cmpres(E%upcase(rror): The following user specified value for StartJob
         (&StartJob.) is > #records in DownloadMacroParms) ;
    %goto FINISH ;
  %end ;

  %Run_Parallel_Jobs(ParallelMethod     = &ParallelMethod.,
                     MacroToRun         = Download_UDM_Data,
                     MacroParmsDs       = DownloadMacroParms,
                     VarsToIgnore       = iteration incr start end numDays,
                     IncludeFile        = &IncludeFile.,
                     MaxNumParallelJobs = &MaxNumParallelJobs.,
                     StartJob           = &startjob.,
                     RerunJobList       = &RerunJobList.,
                     JobTimeoutSec      = &JobTimeoutSec.,
                     RunFolder          = &RunFolder.,
                     AutoexecFile       = &AutoexecFile.,
                     ConfigFile         = &ConfigFile.,
                     OutJobSummaryDs    = &OutJobSummaryDs.,
                     OutCheckLogDs      = &OutCheckLogDs.,
                     ConnectOptions     = &ConnectOptions.) ;

  %let NumIter = %nobs(DownloadMacroParms) ;
  proc sql noprint ;
    select distinct OutFiles2ReadNm into: files2readList separated by " " from DownloadMacroParms ;
    select distinct OutSchemaNm into: SchemaList separated by " " from DownloadMacroParms ;
    select distinct OutTimeStampsNm into: TimeStampList separated by " " from DownloadMacroParms ;
  quit ;

  data &outlib..files2read (compress=YES);
    set %do _i_ = 1 %to &NumIter. ; &outlib..%scan(&files2readlist.,&_i_.,%str( )) %end ;;
  run ;

  data timeStampsALL ;
    set %do _i_ = 1 %to &NumIter. ; &outlib..%scan(&TimeStampList.,&_i_.,%str( )) %end ;;
  run ;
    
  proc sort data=&outlib..files2Read ;
    by EntityName dataRangeStart_dt dataRangeEnd_dt ;
  run ;

  proc sort NODUPKEY data=&outlib..files2Read (keep=EntityName) out=EntityList ; 
    by EntityName ;
  run ;

  proc sql noprint ;
    select min(dataRangeStart_dt) format DATETIME27. into :Min_Start_dt from &outlib..files2Read ;
    select max(dataRangeEnd_dt) format DATETIME27.   into :Max_End_dt from &outlib..files2Read ;
  quit ;

  %let Min_Start_dt = %trim(%left(&Min_Start_dt.)) ;
  %let Max_End_dt = %trim(%left(&Max_End_dt.)) ;

  proc sql noprint ;
    select distinct schemaVersion into: schemaVersion separated by " " from timeStampsALL ;
  quit ;

  data _TimeStamps_ ;
    length schemaVersion $3. ;
    schemaVersion = "&schemaVersion." ;
    dataRangeStart_dt = input("&Min_Start_dt.",DATETIME27.) ; 
    dataRangeEnd_dt = input("&Max_End_dt.",DATETIME27.) ; 
    format dataRangeStart_dt dataRangeEnd_dt DATETIME27.   ;
  run ;

  proc sql noprint ;
    create table &outlib..TimeStamps as select * from EntityList, _TimeStamps_ 
    order by entityName ;
  quit ;

  %do _i_ = 1 %to &NumIter. ;
    %let schemads = %scan(&SchemaList.,&_i_.,%str( )) ;
    %if (%nobs(&outlib..&schemads.) > 0) %then %do ;
      data &outlib..schema ;
        set &outlib..&schemads. ;
      run ;

      %let _i_ = %eval(&Numiter. + 1) ;
    %end ;
  %end ;

  ** move iteration sas datasets to the iteration folder **;
  libname outIter_ "&OutIterFilePath." ;

  proc datasets nolist;
    copy in=&outlib. out=outIter_ memtype=data move;
    select &files2readList. &SchemaList. &TimeStampList. ;
  quit;

  %FINISH:

%mend ;
