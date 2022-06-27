/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Run_Parallel_Jobs.sas
/ Author    : Noah Powers
/ Created   : 2019
/ Updated   : 03-09-2021
/ Purpose   : This macro provides a generic framework to execute multiple invocations of a SAS
/             macro in parallel.  The user provides the name of the macro to be run and a
/             SAS dataset where each record contains a set of macro parameter values to be
/             passed to the macro.
/ FuncOutput: NA
/ Usage     : %Run_Parallel_Jobs(MacroToRun=Combine_Proc_Mixed_Coeffs,
/                   MacroParmsDs=parmsds,
/                   MaxNumParallelJobs=,
/                   IncludeFile=C:\My Documents\Noah Data\Temp\core sas statements.sas,
/                   RunFolder=C:\temp)
/
/ Notes     : - This macro is designed for CPU-bound processes where the overall process
/               performance can be improved by enabling more CPUs to work concurrently on
/               parts of the process.  If your process is I/O bound you may not find any
/               performance improvement by using this macro to parallelize your macro calls.
/             - Will get a non-zero return code from Windows if any task is killed and this does
/               not adversely affect the overall macro run.
/             - This code only makes sense to use when the macro calls it generates do not 
/               depend on the other macro calls
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ ParallelMethod     (systask or connect) This is the technology that will be used to run
/                    the macro invocations in parallel.  There are two options: systask OR connect.
/                    The default is systask for backward compatibility. Systask uses the SAS SYSTASK 
/                    command to spawn separate indpendent sas jobs but this requires that XCMD be enabled.
/                    Alternatively, if SAS/Connect is licensed and installed, this option can be 
/                    used instead.
/ MacroToRun         The name of the SAS macro that will be invoked with the parameters found
/                    in the MACROPARMSDS dataset.  This macro is expected to have all
/                    keyword parameters (none are positional).
/ MacroParmsDs       The name of the SAS dataset that contains a variable for each parameter
/                    required by MACROTORUN.  Additional variables are allowed and these
/                    will be included in the OUTJOBSUMMARYDS data set - but these 
/                    must be specified on the VARSTOIGNORE macro parameter.
/ VarsToIgnore       (optional) A space delimited list of variables in MACROPARMSDS that
/                    are not paramter values to be passed to MACROTORUN.  The typical use
/                    for this is to enable additional info about each job in the job summary
/                    output that is not needed by the MACROTORUN macro.
/ MaxNumParallelJobs The maximum number of jobs to be run at the same time.  This is designed
/                    to prevent the machine running the code from getting overburdened by too
/                    many concurrent processes.
/ StartJob           (default = 1).  This parameter can be set to a value > 1 in the event
/                    that a previous run of this macro only partially completed up to job
/                    Startjob -1.
/ RerunJobList       (optional) A spaced delimited list of job numbers to run. If this is 
/                    provided, the StartJob parameter is ignored.  This parameter is 
/                    intended to be used when some of the jobs failed in a prior. 
/                    When this parameter is non-null then the OutJobSummaryDs OutCheckLogDs
/                    datasets are NOT deleted as these are expected to contain records
/                    for prior successfully run jobs 
/ JobTimeoutSec      (default 0 = no timeout)  This parameter is used in the TIMEOUT= 
/                    parameter on the systask or rsubmit statement to cause the job to end 
/                    after JOBTIMOUTSEC seconds even if the job is not complete.
/ IncludeFile        (optional) An SAS program file that contains SAS statements that will
/                    be included and executed prior to each macro invocation.  The intent is
/                    to enable libnames, filenames, etc that will be required for every
/                    macro invocation.
/ RunFolder          The folder where the individual SAS programs that contain the macro
/                    invocations, the log & listing files associated with each program will
/                    be stored.
/ ConfigFile         (optional) The full file path and name of the SAS configuration file
/                    that will be used for each macro invocation.  If this is not provided then
/                    each macro invocation will use the same config file that the parent SAS
/                    process uses.
/ AutoexecFile       (optional) The full file path and name of the SAS autoexec file
/                    that will be used for each macro invocation.  If this is not provided then
/                    each macro invocation will use the same autoexec file that the parent SAS
/                    process uses.
/ WorkFolder         The folder that the parallel jobs will in which the work is pointed.  
/                    The default is missing.
/ CmdLineOptions     (optional) If this is provided it is expected to be valid commmand line
/                    options that will be added to the parallel job invocation.  There are
/                    other macro options available to force the parallel jobs to be non-interacative
/                    as well as to control the following settings:
/                     - autoexec
/                     - config
/                     - work
/                     - log
/                     - print
/      
/                    Note that if an invalid option is provided no logs may be produced.
/ OutJobSummaryDs    The name of the summary output dataset created that contains the
/                    start time, finish time, run time and return code (from the OS) for each
/                    job or macro invocation run by this macro.  It also includes the parameters
/                    for each job from the MACROPARMSDS dataset.  The default value for this
/                    parameter is WORK._JOBS_.
/ OutCheckLogDs      The name of the dataset to contain any suspicious records found
/                    when the check log macro is run against each jobs log.  The default
/                    value for this parameter is Check_Log
/ ConnectOptions     Optional settings specified using space delimited list of key words.
/                    Currently, only detects NOSASCMD option for situation where SAS is in
/                    a lockdown state prohibits you from specifying the sascmd on the remote
/                    machines.  When the user specifies NOSASCMD for this parameter, the 
/                    sascmd="" option of the SIGNON commannd for SAS/Connect is dropped as 
/                    this not allowed in a lockdown state. A consequence of this is that 
/                    any of the following user parameters are ignored as well:
/                     - ConfigFile 
/                     - AutoexecFile  
/                     - WorkFolder  
/                     - CmdLineOptions 
/                    As these options are all specified on the sascmd statement.
/============================================================================================
/ High Level Program Summary (Processing Steps)
/ -------------------------------------------------------------------------------------
/ Step  Description
/ -------------------------------------------------------------------------------------
/ 1. Print user supplied macro parameters to the SAS log for QC purposes
/ 2. Validate that the user supplied macro parameters meet expectations
/ 3. Create N (N = the number of observations in MACROPARMSDS) SAS programs that will
/    be executed by this macro
/ 4. Determine the full filepath for the SAS executable, config and autoexec.sas files
/    unless these values are provided by the user
/ 5. Construct the commands to execute the SAS code created in step 3 and submit these
/    until MaxNumParallelJobs are running.  As jobs complete, additional jobs are
/    submitted until all are completed or running.
/ 6. After a job completes, check to see if a change to MaxNumParallelJobs has been 
/    requested via the control file.  If a valid new value is found then update
/    MaxNumParallelJobs.  If the new value is larger then submit more jobs.  If the new
/    value is smaller then do not add any new jobs until enough have completed so that
/    the number of jobs running is < MaxNumParallelJobs - then go back to step 5.
/ 7. Create a summary dataset that lists the start time, finish time, running time,
/    OS return code and number of suspicious lines from Checklog macro scan for each
/    program submitted.  This gets created each time a job completes and this data
/    is appended to the master file.
/ 8. Create a compiled dataset containing the output data sets from Check log all concatenated
/    together and sorted so more serious errors are at the top of the dataset. This gets 
/    created each time a job completes and this data is appended to the master file.
/============================================================================================*/
%macro Run_Parallel_Jobs(ParallelMethod     = systask,
                         MacroToRun         = ,
                         MacroParmsDs       = ,
                         VarsToIgnore       = ,
                         MaxNumParallelJobs = ,
                         StartJob           =1,
                         RerunJobList       =,
                         JobTimeoutSec      =0,
                         IncludeFile        = ,
                         RunFolder          = .,
                         ConfigFile         = ,
                         AutoexecFile       = ,
                         WorkFolder         = ,
                         CmdLineOptions     = ,
                         OutJobSummaryDs    = _jobs_,
                         OutCheckLogDs      = Check_Log,
                         ConnectOptions     =) ;

  **---------------------------------------------------------------------------------------**;
  ** Print the macro parameter names and user provided values to the log (for QC purposes) **;
  **---------------------------------------------------------------------------------------**;

  data _null_ ;
    set sashelp.vmacro (where=(scope="&sysmacroname.")) end=lastrec ;
    if _N_ = 1 then do ;
      put "%sysfunc(repeat(-, 42 + %length(&sysmacroname.)))" ;
      put "---- Macro parameter values passed to &sysmacroname. ----" ;
      put "%sysfunc(repeat(-, 42 + %length(&sysmacroname.)))" ;
    end ;
    put name "= " value ;
    if lastrec then put "%sysfunc(repeat(-, 42 + %length(&sysmacroname.)))" ;
  run ;

  %local MacroName MacroParameterList NumMacroParameters job i parm sasRoot SASautoexecPath
         SASConfigPath SASExecPath RunningJobs sascmd CompletedJobs task slash JobNumFormat
         quoteOption MaxNumParallelJobs_New startTime startTimeDesc CurrentTime parm_fmt 
         Jobs2Remove xmprint xnotes JobSummaryDsLib JobSummaryDsName CheckLogDsLib 
         CheckLogDsName fmt MKDIR_DIR base_path program_path MaxNumJobs JobList iter signon_opts ;
  %let MacroName = &sysmacroname.;

  %let slash = / ;
  %if "%upcase(&sysscp.)" = "WIN" %then %let slash = \ ;

  **--------------------------------------------------**;
  **     Validate user inputs passed to macro         **;
  **--------------------------------------------------**;

  %let startTime = %sysfunc(datetime()) ;
  %let startTimeDesc = %sysfunc(putn(&startTime.,dateampm22.)) ;

  %let quoteOption = %sysfunc(getoption(QUOTELENMAX));
  %let xmprint = %sysfunc(getoption(mprint)) ;        
  %let xnotes = %sysfunc(getoption(notes)) ;                
  options NoQuoteLenMax mprint ;
  
  %if (not %length(&MacroToRun.)) OR (not %length(&MacroParmsDs.)) OR (not %length(&OutJobSummaryDs.))
      OR (not %length(&OutCheckLogDs.)) %then %do ;
    %put %cmpres(E%upcase(rror): (&MacroName.) One or more of the following required parameters have
         Null values: MacroToRun MacroParmsDs OutJobSummaryDs OutCheckLogDs.) ;
    %goto FINISH ;
  %end ;

  %if (not %length(&ParallelMethod.)) %then %do ;
    %let ParallelMethod = systask ;
  %end ;
  %let ParallelMethod = %upcase(&ParallelMethod.) ;

  %if ("&ParallelMethod." NE "SYSTASK") AND ("&ParallelMethod." NE "CONNECT") %then %do ;
    %put %cmpres(E%upcase(rror): (&MacroName.) Unexpected value for ParallelMethod=&ParallelMethod.) ;
    %goto FINISH ;
  %end ;

  %if not %sysfunc(exist(&MacroParmsDs.)) %then %do ;
    %put %cmpres(E%upcase(rror): (&MacroName.) The following user specified value for MacroParmsDs
         (&MacroParmsDs.) does not exist.) ;
    %goto FINISH ;
  %end ;
  
  %if not %length(&StartJob.) %then %let StartJob = 1 ;
  %if %eval(&StartJob. > %nobs(&MacroParmsDs.)) %then %do ;
    %put %cmpres(E%upcase(rror): (&MacroName.) The following user specified value for StartJob
         (&StartJob.) is > #records in &MacroParmsDs.) ;
    %goto FINISH ;
  %end ;
  
  %if not %length(&RunFolder.) %then %let RunFolder = . ;
  %else %if not %sysfunc(fileexist(&RunFolder.)) %then %do ;
    %put %cmpres(E%upcase(rror): (&MacroName.) The following user specified path for RUNFOLDER
         (&runFolder.) does not exist.) ;
    %goto FINISH ;
  %end ;

  %if %length(&VarsToIgnore.) %then %do ;
    %if NOT %hasvars(&MacroParmsDs.,&VarsToIgnore.) %then %do ;
      %put %cmpres(E%upcase(rror): (&MacroName.) The following variable(s) listed in VARSTOIGNORE 
            were not found in &MacroParmsDs.: &_nomatch_.) ;
      %goto FINISH ;
    %end ;
  %end ;

  %if %length(&WorkFolder.) %then %do;
    %if %eval(not(%sysfunc(fileexist("%nrquote(%superq(WorkFolder))")))) %then %do ;
      %put %cmpres(E%upcase(rror): (&MacroName.) The following user specified path for
                   WORKFOLDER (&WorkFolder.) does not exist or could not be created.) ;
      %goto FINISH ;
    %end;                                   
  %end;

  %if %length(&includeFile.) %then %do ;
    %if not %sysfunc(fileexist(&includeFile.)) %then %do ;
      %put %cmpres(E%upcase(rror): (&MacroName.) The following user specified path for INCLUDEFILE
           (&IncludeFile.) does not exist.) ;
      %goto FINISH ;
    %end ;
  %end ;

  %if %length(&ConfigFile.) %then %do ;
    %if not %sysfunc(fileexist(&ConfigFile.)) %then %do ;
      %put %cmpres(E%upcase(rror): (&MacroName.) The following user specified path for CONFIGFILE
           (&ConfigFile.) does not exist.) ;
      %goto FINISH ;
    %end ;
  %end ;

  %if %length(&AutoexecFile.) %then %do ;
    %if not %sysfunc(fileexist(&AutoexecFile.)) %then %do ;
      %put %cmpres(E%upcase(rror): (&MacroName.) The following user specified path for AUTOEXECFILE
           (&AutoexecFile.) does not exist.) ;
      %goto FINISH ;
    %end ;
  %end ;

  %** if the max number of parallel jobs is not specified the use the CPUCOUNT SAS system option **;
  %if NOT %length(&MaxNumParallelJobs.) %then %do;
    %let MaxNumParallelJobs = %sysfunc(getoption(CPUCOUNT));
    %put %cmpres(%upcase(Note): (&MacroName.) The user did not provide a value for the
           MaxNumParallelJobs macro parameter.  The value of CPUCOUNT SAS option
           (&MaxNumParallelJobs.) will be used instead.) ;
  %end;

  **--------------------------------------------------**;
  **                   Main Body                      **;
  **--------------------------------------------------**;

  %if ("&ParallelMethod." = "SYSTASK") %then %do ;
    SYSTASK KILL _ALL_ ;
  %end ;
  %else %do ;
    KILLTASK _ALL_ ;
  %end ;
  
  %let MaxNumJobs = %nobs(&MacroParmsDs.) ;
  
  %if %length(&RerunJobList.) %then %do ;
    %let JobList = &RerunJobList. ;
  %end ;
  %else %do ;
    %let JobList = ;
    %do i = &StartJob. %to &MaxNumJobs. ;
      %let JobList = &JobList. &i. ;
    %end ;  
  %end ;

  %let NumJobs = %words(&JobList.) ;
  %let JobNumFormat = z%length(&NumJobs.). ;

  data _MacroParmsDs_ ;
    set &MacroParmsDs. (drop=&VarsToIgnore.) ;
  run ;

  %let MacroParameterList = %varlist(_MacroParmsDs_) ;
  %let NumMacroParameters = %words(&MacroParameterList.) ;

  %let lastchar = %substr(%trim(&RunFolder.),%length(%trim(&RunFolder.)),1) ;
  %if "&lastchar." = "/" or "&lastchar." = "\" %then
        %let RunFolder = %substr(%trim(&RunFolder.), 1 , %length(%trim(&RunFolder.))-1) ;

  %let ParallelJobsControlFile = &RunFolder.&slash.&macrotorun._%sysfunc(putn(0,&JobNumFormat.))_Num_Job_Control_File_&SYSJOBID..txt ;

  ** Set up initial Control file **;
  filename _cont_ "&ParallelJobsControlFile." ;

  data _null_ ;
    file _cont_ ;
    put "&MaxNumParallelJobs." ;
  run ;

  ** Create the SAS program text files that will be executed **;
  data _null_ ;
    length SASpgmPath printto $1000. macrCall $32000 ;
    set &MacroParmsDs. ;
    if _N_ in (&JobList.) ;
    SASpgmPath = "&RunFolder.&slash.&macrotorun._" || trim(left(put(_N_,&JobNumFormat.))) || ".sas" ;
    file dummy filevar=SASpgmPath LRECL=32000 ;

    %if ("&ParallelMethod." = "CONNECT") %then %do ;
      printto = "proc printto log='" || "&RunFolder.&slash.&macrotorun._" || trim(left(put(_N_,&JobNumFormat.))) || ".log' " ||
                "print='&RunFolder.&slash.&macrotorun._" || trim(left(put(_N_,&JobNumFormat.))) || ".lst' NEW ;" ;
      put printto ;
    %end ;
   
    ** Add code to each job to capture PID related info in the SAS log **;
    put ;
    put 'data _null_ ;' ;
    put '  length msgstr $200. ;' ;
    put '  put ;' ;
    put '  msgstr = repeat("*",90) ;' ;
    put '  put msgstr ;' ;
    put '  msgstr = "* System Process ID (PID) for this batch job" ;' ;
    put '  put msgstr ;' ;
    put '  msgstr = repeat("*",90) ;' ;
    put '  put msgstr ;' ;
    put '  put ;' ;
    put '  msgstr = "  PID                      : &SYSJOBID." ;' ;
    put '  put msgstr ;' ;
    put '  msgstr = "  SAS Temporary Directory  : _TD&SYSJOBID." ;' ;
    put '  put msgstr ;' ;
    put '  msgstr = "  SAS Temporary Full Path  : %sysfunc(pathname(WORK))" ;' ;
    put '  put msgstr ;' ;
    put '  put ;' ;
    put '  msgstr = repeat("*",90) ;' ;
    put '  put msgstr ;' ;
    put '  put ;' ;
    put 'run ;' ;
    put ;

    %if %length(&VarsToIgnore.) %then %do ;
      ** Add the values as comments to the program **;
      put '/*' ;
      put ' ' ;
      put "Non-macro parameter variables in &MacroParmsDs.:" ;
      put ' ' ;
      %do i = 1 %to %words(&VarsToIgnore.) ;
        put %scan(&VarsToIgnore.,&i.,%str( ))= ;
      %end ;
      put ' ' ;
      put '*/' ;
      put ;
    %end ;

    %if %length(&includefile.) %then %do ;
      put "%include ""&includefile."" ;" ;
      put ;
    %end ;

    macrCall = "%" || "&macrotorun." || "("  ;
    put macrCall ;
    %do i = 1 %to &NumMacroParameters. ;
      %let parm = %scan(&MacroParameterList.,&i.,%str( )) ;
      %if %vartype(&MacroParmsDs.,&parm.) = N %then %do ;
        %let fmt = %varformat(&MacroParmsDs.,&parm.) ;
        %if not %length(&fmt.) %then %let fmt = best32. ;
        %let parm_fmt = put(&parm.,&fmt.) ;
      %end ;
      %else 
        %let parm_fmt = &parm. ;
      %if %eval(&i. < &NumMacroParameters.) %then %do ;
        macrCall = "&parm." ;
        put @%eval(%length(&macrotorun.)+3) macrCall @@ ;
        macrCall = "= " || trim(left(&parm_fmt.)) || "," ;
        put @%eval(%length(&macrotorun.)+ 36) macrCall ;
      %end ;
      %else %do ;
        macrCall = "&parm." ;
        put @%eval(%length(&macrotorun.)+3) macrCall  @@ ;
        macrCall = "= " || trim(left(&parm_fmt.)) ;
        put @%eval(%length(&macrotorun.)+ 36) macrCall  ;
      %end ;
    %end ;
    macrCall = repeat(" ",%eval(%length(&macrotorun.)+1)) || ");" ;
    macrCall = trim(put(macrCall,$char32000.)) ;
    put macrCall ;  

    %if ("&ParallelMethod." = "CONNECT") %then %do ;
      put ;
      put "proc printto ; run ;" ;
    %end ;

    format macrCall $char32000. ;
  run ;

  ** determine the sasroot path from the dictionary tables ;
  proc sql noprint;
    select trim(left(path)) into :sasRoot from dictionary.members where libname='SASROOT';
    %if not %length(&AutoexecFile.) %then %do ;
      select trim(left(setting)) into: AutoexecFile from sashelp.vallopt
        where upcase(optname)="AUTOEXEC" ;
    %end ;
    %if not %length(&ConfigFile.) %then %do ;
      select trim(left(setting)) into: ConfigFile from sashelp.vallopt
        where upcase(optname)="CONFIG" ;
    %end ;
  quit;

  ** Need to handle instances when CONFIG file was not specified by the user **;
  %if "%substr(&ConfigFile.,1,1)" = "(" %then
    %let ConfigFile = ;

  * set values for SASExecPath and SASConfigPath to reference in the systask command ;
  data _null_;
    length path_name $ 256;
    %if "%upcase(&sysscp.)" = "WIN" %then %do ;
      path_name = "%trim(&sasRoot)" || "&slash.sas" ;
    %end ;
    %else %do ;
      path_name = "%trim(&sasRoot)/sasexe" || "&slash.sas" ;
    %end ;
    call symputx("SASExecPath",path_name);
  run;

  **------------------------------------------------------------**;
  ** delete any existing output datasets for later proc appends **;
  **------------------------------------------------------------**;
  
  %if %length(&RerunJobList.) <= 0 %then %do ;

    %let JobSummaryDsLib  = %scan(&OutJobSummaryDs.,1,%str(.)) ;
    %let JobSummaryDsName = %scan(&OutJobSummaryDs.,2,%str(.)) ;
  
    %if not %length(&JobSummaryDsName.) %then %do ;
      %let JobSummaryDsLib  = WORK ;
      %let JobSummaryDsName = &OutJobSummaryDs. ;
    %end ;
  
    proc datasets library=&JobSummaryDsLib. NOLIST ;
      delete &JobSummaryDsName. / memtype=DATA ;
    quit ;
  
    %let CheckLogDsLib  = %scan(&OutCheckLogDs.,1,%str(.)) ;
    %let CheckLogDsName = %scan(&OutCheckLogDs.,2,%str(.)) ;
  
    %if not %length(&CheckLogDsName.) %then %do ;
      %let CheckLogDsLib  = WORK ;
      %let CheckLogDsName = &OutCheckLogDs. ;
    %end ;
  
    proc datasets library=&CheckLogDsLib. NOLIST ;
      delete &CheckLogDsName. / memtype=DATA ;
    quit ;
  
  %end ;
  
  **---------------------------------------**;
  ** Main Job Submission and tracking loop **;
  **---------------------------------------**;

  %let RunningJobs = ;
  %do iter = 1 %to &NumJobs. ;
    %let job = %scan(&JobList.,&iter.,%str( )) ;
    %local start&job. finish&job. CheckLogLines&job. ;
    
    ** build up the sas command (nested quotes are easier using a data step) **;
    data _null_;
      length sascmd $10000. ;
      sascmd =
      "'&SASExecPath.'" ||
      %if ("&ParallelMethod." = "SYSTASK") %then %do ;
        " -sysin '" || "&RunFolder.&slash.&macrotorun._%sysfunc(putn(&job.,&JobNumFormat.)).sas" || "'" ||
        " -log '&RunFolder.&slash.&macrotorun._%sysfunc(putn(&job.,&JobNumFormat.)).log" || "'" ||
        " -print '&RunFolder.&slash.&macrotorun._%sysfunc(putn(&job.,&JobNumFormat.)).lst" || "'" ||
        " -sysparm &job. " ||
      %end ;
      %if %length(&ConfigFile.) %then %do ;
        " -config '" || "%trim(&ConfigFile.)" || "'" ||
      %end ;
      %if %length(&AutoexecFile.) %then %do ;
        " -autoexec '" || "%trim(&AutoexecFile.)" || "'" ||
      %end ;
      %if %length(&WorkFolder.) %then %do;
        " -work '" || "%trim(&WorkFolder.)" || "'" ||
      %end;
      %if %length(&CmdLineOptions.) %then %do;
        " &CmdLineOptions." ||
      %end;
      %if "%upcase(&sysscp.)" = "WIN" %then %do ;
        " -noterminal -nostatuswin -nosplash "
      %end ;
      %else %do ;
        " -noterminal -nodms "
      %end ;
      ;
      call symput('sascmd', trim(left(sascmd)));
    run;
    
    %if %length(&ConnectOptions.) %then %do ;
      %if %index(%upcase(&ConnectOptions.),"NOSASCMD") > 0 %then %do ;
        %let signon_opts = %str();
      %end ;
      %else %do ;
         %let signon_opts = %str(sascmd="&sascmd.") ; 
      %end ;
    %end ;
    
    %if ("&ParallelMethod." = "SYSTASK") %then %do ;
      systask command "&sascmd." taskname=task&job. status=taskRC&job. ;
      %put SYSTASK submitted for task=&job. SYSRC=&SYSRC. ;
    %end ;
    %else %do ;

      %let base_path = &RunFolder.&slash.&macrotorun._%sysfunc(putn(&job.,&JobNumFormat.)) ;
      %let program_path = &base_path..sas ;

      SIGNON Task&job. SIGNONWAIT=YES &signon_opts. ;
        %syslput rem_program_path=&program_path. / remote=Task&job.;
        rsubmit Task&Job. wait=NO  cmacvar=taskRC&job. log=PURGE output=PURGE;
          OPTIONS SOURCE SOURCE2 MPRINT ;
          %include "&rem_program_path." / source2 ; 
        endrsubmit ;
    %end ;

    %let start&job. = %sysfunc(datetime()) ;
    %let RunningJobs = &RunningJobs. &job. ;
    %let CurrentTime = %sysfunc(datetime()) ;

    %do i = 1 %to %words(&RunningJobs.) ;
      %if ("&ParallelMethod." = "SYSTASK") %then %do ;
        systask list task%scan(&RunningJobs.,&i.,%str( )) STATE STATVAR ;
      %end ;
      %else %do ;
        listtask task%scan(&RunningJobs.,&i.,%str( )) ;
      %end ;
    %end ;

    %WAIT:

    %if %words(&RunningJobs.) >= &MaxNumParallelJobs. and &iter. < &Numjobs. %then %do ;
      waitfor %do i = 1 %to %words(&RunningJobs.) ; task%scan(&RunningJobs.,&i.,%str( )) %end ; TIMEOUT=&JobTimeoutSec. ;

      ** Determine which job(s) are done and remove from list of running jobs **;
      %*put  #preloop RunningJobs = "&RunningJobs." ;
      %*put  #preloop task = "&task." ;
      %let Jobs2Remove = ;
      %put JOB=&job. Exited waitfor statement ;
      %do i = 1 %to %words(&RunningJobs.) ;
        %let task = %scan(&RunningJobs.,&i.,%str( ));
         %*put #inloop(i=&i) RunningJobs = "&RunningJobs." ;
         %*put #inloop(i=&i) task = "&task." ;
        %put task&task. Status = &&taskRC&task. ;
        %if ( %length(&&taskRC&task.) AND ("&&taskRC&task." ne ".") AND ("&ParallelMethod." = "SYSTASK") ) OR 
            (%eval(&&taskRC&task. = 0) AND ("&ParallelMethod." = "CONNECT")) %then %do ;
          %let CompletedJobs = &CompletedJobs. &task. ;
          %let Jobs2Remove = &Jobs2Remove. &task. ;
          %let Finish&task. = %sysfunc(datetime()) ;
          
          %if ("&ParallelMethod." = "CONNECT") %then %do ;
            signoff Task&task. cmacvar=signoff&task.;
          %end ;

          options nonotes nomprint ;

          %check_Log(
             filepath       = &RunFolder.&slash.&macrotorun._%sysfunc(putn(&task.,&JobNumFormat.)).log,
             Print          = N,
             outds          = Check_Log&task.,
             Run_Time_Check = ) ;

          options &xnotes. mprint ;

          %let CheckLogLines&task. = %nobs(Check_Log&task.) ;

          ** Write the status for each completed task to the log **;
          %put ***----------------------------------------------------------*** ;
          %put NOTE: Job completed: &task. with OS Return Code: &&taskRC&task. ;
          %put NOTE: &&CheckLogLines&task. suspicious lines in the SAS log ;
          %put NOTE: Currently there are %eval(%words(&RunningJobs.)-%words(&Jobs2Remove.)) jobs executing ;
          %put NOTE: Now a total of %words(&CompletedJobs.) job(s) completed of &NumJobs. ;
          %put NOTE: &macroName. Start Time   : %left(&startTimeDesc.) ;
          %put NOTE: &macroName. Current Time : %left(%sysfunc(putn(&CurrentTime.,dateampm22.))) ;
          %put NOTE: &macroName. Elapsed Time : %trim(%left(%sysfunc(putn(%sysevalf(( &CurrentTime. - &startTime. ) / 3600),comma10.2)))) Hours ;
          %put ***----------------------------------------------------------*** ;

          data _JobSummary&task._ ;
            length _jobNumber_ _StartTime_ _FinishTime_ _RC_ CheckLogLines _RunTimeMins_ 8. ;
            set &MacroParmsDs. (firstobs=&task. obs=&task.) ;
            _jobNumber_ = &task. ;
            _StartTime_ = &&start&task. ;
            _FinishTime_ = &&finish&task. ;
            _RC_ = &&taskRC&task. ;
            CheckLogLines = &&CheckLogLines&task. ;
            _RunTimeMins_ = intck('SECOND', _StartTime_, _FinishTime_) / 60 ;
            format _StartTime_ _FinishTime_ dateampm22.2 _RunTimeMins_ comma7.2 ;
          run ;

          proc append data=_JobSummary&task._ base=&OutJobSummaryDs. ;
          run ;

          data Check_Log&task. ;
            set Check_Log&task. ;
            job = &task. ;
          run ;

          proc append data=Check_Log&task. base=&OutCheckLogDs. ;
          run ;

        %end ;
      %end ;

      %let RunningJobs = %removeWords(&RunningJobs., &Jobs2Remove.) ;

      ** Check to see if a change to MaxNumParallelJobs has been requested via the **;
      ** control file.  If so act accordingly **;

      %if %sysfunc(fileexist(&ParallelJobsControlFile.)) %then %do ;

        data _null_ ;
          infile _cont_ obs=1 ;
          input MaxNumParallelJobs @1 ;
          MaxNumParallelJobs = round(MaxNumParallelJobs,1) ;
          if  100 >= MaxNumParallelJobs >= 1 then
            call symput("MaxNumParallelJobs_New",trim(left(put(MaxNumParallelJobs,10.)))) ;
          else 
            put "W%upcase(arning): Invalid new value for MaxNumParallelJobs found: " MaxNumParallelJobs 
                "this will be ignored." ;
        run ;

        %if %length(&MaxNumParallelJobs_New.) and %eval(&MaxNumParallelJobs_New. NE &MaxNumParallelJobs.) %then %do ;
          %put ***----------------------------------------------------------*** ;
          %put NOTE: New value for MaxNumParallelJobs found in control file ;
          %put NOTE: Control File: &ParallelJobsControlFile. ;
          %put NOTE: Previous Value: &MaxNumParallelJobs. ;
          %put NOTE: New Value: &MaxNumParallelJobs_New. ;
          %put ***----------------------------------------------------------*** ;
          %let MaxNumParallelJobs = &MaxNumParallelJobs_New. ;
        %end ;
      %end ;
    %end ;
    
    %else %if &iter. = &Numjobs. %then %do %while (%length(&RunningJobs.) > 0) ;
      waitfor %do i = 1 %to %words(&RunningJobs.) ; task%scan(&RunningJobs.,&i.,%str( )) %end ; TIMEOUT=&JobTimeoutSec. ;
      
      %let Jobs2Remove = ;
      %do i = 1 %to %words(&RunningJobs.) ;
        %let task = %scan(&RunningJobs.,&i.,%str( ));
        %put task&task. Status = &&taskRC&task. ;
        
        %if (%length(&&taskRC&task.) AND ("&&taskRC&task." ne ".") AND ("&ParallelMethod." = "SYSTASK")) OR 
           (%eval(&&taskRC&task. = 0) AND ("&ParallelMethod." = "CONNECT")) %then %do ;
          %let CompletedJobs = &CompletedJobs. &task. ;
          %let Jobs2Remove = &Jobs2Remove. &task. ;
          %let Finish&task. = %sysfunc(datetime()) ;
          
          %if ("&ParallelMethod." = "CONNECT") %then %do ;
            signoff Task&task. cmacvar=signoff&task.;
          %end ;

          options nonotes nomprint ;
  
          %check_Log(
             filepath       = &RunFolder.&slash.&macrotorun._%sysfunc(putn(&task.,&JobNumFormat.)).log,
             Print          = N,
             outds          = Check_Log&task.,
             Run_Time_Check = ) ;
  
          options &xnotes. mprint ;
  
          %let CheckLogLines&task. = %nobs(Check_Log&task.) ;
  
          ** Write the status for each completed task to the log **;
          %put ***----------------------------------------------------------*** ;
          %put NOTE: Job completed: &task. with OS Return Code: &&taskRC&task. ;
          %put NOTE: &&CheckLogLines&task. suspicious lines in the SAS log ;
          %put NOTE: Currently there are 0 jobs executing ;
          %put NOTE: Now a total of %words(&CompletedJobs.) job(s) completed of &NumJobs. ;
          %put NOTE: &macroName. Start Time   : %left(&startTimeDesc.) ;
          %put NOTE: &macroName. Current Time : %left(%sysfunc(putn(&CurrentTime.,dateampm22.))) ;
          %put NOTE: &macroName. Elapsed Time : %trim(%left(%sysfunc(putn(%sysevalf(( &CurrentTime. - &startTime. ) / 3600),comma10.2)))) Hours ;
          %put ***----------------------------------------------------------*** ;
  
          data _JobSummary&task._ ;
            length _jobNumber_ _StartTime_ _FinishTime_ _RC_ CheckLogLines _RunTimeMins_ 8. ;
            set &MacroParmsDs. (firstobs=&task. obs=&task.) ;
            _jobNumber_ = &task. ;
            _StartTime_ = &&start&task. ;
            _FinishTime_ = &&finish&task. ;
            _RC_ = &&taskRC&task. ;
            CheckLogLines = &&CheckLogLines&task. ;
            _RunTimeMins_ = intck('SECOND', _StartTime_, _FinishTime_) / 60 ;
            format _StartTime_ _FinishTime_ dateampm22.2 _RunTimeMins_ comma7.2 ;
          run ;
  
          proc append data=_JobSummary&task._ base=&OutJobSummaryDs. ;
          run ;
  
          data Check_Log&task. ;
            set Check_Log&task. ;
            job = &task. ;
          run ;
  
          proc append data=Check_Log&task. base=&OutCheckLogDs. ;
          run ;
          
        %end ;      
      %end ;      
      
      %let RunningJobs = %removeWords(&RunningJobs., &Jobs2Remove.) ;
    %end ;

    %** Dont submit any new jobs until under the new limit **;
    %if %eval(%words(&RunningJobs.) >= &MaxNumParallelJobs.) %then %do ;
      %put RunningJobs=&RunningJobs. ;
      %put MaxNumParallelJobs=&MaxNumParallelJobs. ;
      %put Entered section to be redirected to WAIT ;
      %goto WAIT ;
    %end ;

  %end ;

  proc sort data=&OutCheckLogDs. ; by ErrLevel job lineNum ; run ;
  proc sort data=&OutJobSummaryDs. ; by _jobnumber_ ; run ;

  %FINISH:
 
  %let CurrentTime = %sysfunc(datetime()) ;
  %put ***----------------------------------------------------------*** ;
  %put NOTE: Currently there are %words(&RunningJobs.) jobs executing ;
  %put NOTE: %words(&CompletedJobs.) jobs completed of &NumJobs. ;
  %put NOTE: &macroName. Start Time   : %left(&startTimeDesc.) ;
  %put NOTE: &macroName. Current Time : %left(%sysfunc(putn(&CurrentTime.,dateampm22.))) ;
  %put NOTE: &macroName. Elapsed Time : %trim(%left(%sysfunc(putn(%sysevalf(( &CurrentTime. - &startTime. ) / 3600),comma10.2)))) Hours ;
  %put ***----------------------------------------------------------*** ;

  options &quoteOption.;
  options &xnotes. &xmprint. ;

%mend ;