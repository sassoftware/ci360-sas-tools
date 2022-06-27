/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Log_Run_Time.sas
/ Author    : Noah Powers
/ Created   : 2019
/ Purpose   : Create a SAS dataset with the real and cpu time for each procedure and data
/             step found in a SAS log.  The information is pulled from either a saved
/             SAS log file or the log window.  The intended use for this code is to be
/             able to locate the procedure and/or data steps in SAS code that are taking
/             the most time.
/ FuncOutput: NA
/ Usage     :
/ Notes     :
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ LogFile            (opt) The file file path and name of the log file to be read into
/                    a dataset.  If this parameter is not provided by the user then the
/                    log window in SAS is processed.
/ OutDs              The name of the output SAS dataset to create that will contain the
/                    real and cpu time for each data step and procedure in the log.  The
/                    default value is WORK._Log_Run_Time_.
/ ProcOutDs          (opt) The name of the SAS dataset to contain the summary of 
/                    time used by procedure/data step.  The default value is _proc_time_summary_
/============================================================================================*/
%macro Log_Run_Time(LogFile   =,
                    OutDs     =_Log_Run_Time_,
                    ProcOutDs =_proc_time_summary_) ;

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

  %local MacroName source TotalTime infileOpt ;
  %let MacroName = &sysmacroname. ;

  %if not %length(&LogFile.) %then %do ;
    filename temp_log temp ;
    dm log 'file temp_log replace' log ;
    %let infileOpt = truncover ;
  %end ;
  %else %do ;
    ** verify that the log file exists **;
    %if (%sysfunc(fileexist(&LogFile.)) NE 0) %then %do ;
      filename temp_log "&LogFile." ;
    %end ;
    %else %do ;
      %put E%upcse(rror): (&MacroName.) User specified log file (&LogFile.) does not exist. ;
      %goto FINISH ;
    %end ;
    %let infileOpt = ;
  %end ;

  %macro calc_hms() ;
    time = compress(upcase(log_rec)," ABCDEFGHIJKLMNOPQRSTUVWXYZ") ;
    hours = 0 ;
    minutes = 0 ;
    seconds = 0 ;
    if index(log_rec,"seconds") > 0 then do ;
      seconds = input(time,8.) ;
    end ;
    else if length(compress(time,"0123456789.")) = 2 then do ;
      hours = input(substr(time,1,index(time,":")-1),8.) ;
      minutes = input(substr(time,index(time,":")+1,2),8.) ;
      seconds = input(substr(time,index(time,":")+4),8.) ;
    end ;
    else do ;
      minutes = input(substr(time,1,index(time,":")-1),8.) ;
      seconds = input(substr(time,index(time,":")+1),8.) ;
    end ;
  %mend ;

  data &outds. ;
    infile temp_log LRECL=500 &infileOpt. ;
    length mprintstr $100 procedure $32. real_hours cpu_hours 8. ;
    retain mprintStr linenum procedure real_hours cpu_hours ;
    input ;
    log_rec = _infile_ ;
    if log_rec =: "MPRINT(" then do ;
      MprintStr = substr(log_rec,8,index(log_rec,")")-8) ;
    end ;
    if trim(left(log_rec)) =: "NOTE:" and 
      (index(log_rec,"used (Total process time):") > 0 or trim(left(log_rec)) =: "NOTE: SAS initialization used:") 
    then do ;
      if trim(left(log_rec)) =: "NOTE: PROCEDURE" then procedure = scan(substr(log_rec,16),1," ") ;
      else if trim(left(log_rec)) =: "NOTE: SAS initialization used:" then procedure = "SAS INITIALIZATION" ;
      else procedure = "DATA STEP" ;
      linenum = _N_ ;
      real_time = . ;
      cpu_time = . ;
    end ;

    if trim(left(log_rec)) =: "real time" and (index(log_rec,"seconds") > 0 or index(log_rec,":") > 0) then do ;
      %calc_hms() ;
      real_hours = hours + minutes/60 + seconds/3600 ;
    end ;
    else if     (
                        trim(left(log_rec)) =: "cpu time" 
                or      trim(left(log_rec)) =: "user cpu time" 
                )               
                and (index(log_rec,"seconds") > 0 or index(log_rec,":") > 0) then do ;
      %calc_hms() ;
      cpu_hours = hours + minutes/60 + seconds/3600 ;
      output ;
    end ;
    else if trim(left(log_rec)) =: "NOTE: The SAS System used" then stop ;
    else delete ;

    keep linenum cpu_Hours real_Hours MprintStr procedure ; 
  run ;
  
  filename temp_log clear ;

  ** Capture total run time as the sum of all real time in log **;
  proc sql noprint ;
    select      sum(real_Hours),
                sum(cpu_Hours)
                into    :TotalTime ,
                        :TotalCPU       
    from &outds. ;
  quit ;

  %if %length(&logfile.) %then %let source = &logfile. ;
  %else %let source = SAS Log Window ;

  %put ------------------------------------------------------------------------------ ;
  %put Note: (&MacroName.) Total procedure and data step time     : &TotalTime. hours. ;
  %put Note: (&MacroName.) Total procedure and data step CPU time : &TotalCPU. hours. ;
  %put Note: (&MacroName.) Source: &source. ;
  %put ------------------------------------------------------------------------------ ;

  proc sort data=&outds. ; by DESCENDING real_Hours ; run ;

  data &outds. ;
    set &outds. ;
    retain cumPct 0 ;
    if real_Hours > 0 then pctCPUTime = cpu_Hours / real_Hours ;
    if (&totalTime. > 0) then pctTime = real_Hours / &TotalTime. ;
    cumPct = cumPct + pctTime ;
    format pctCPUTime pctTime cumPct percent8.1 cpu_hours real_hours comma8.2  ;
    label linenum     = "Line # in Log file"
          real_hours  = "Total Time in Hours"
          cpu_hours   = "CPU Time in Hours"
          pctCPUTime  = "Pct of total data step/proc time taken by CPU"
          pctTime     = "Pct of ALL data step/proc time"
          cumPct      = "Cumulative Pct of ALL data step/proc time"
          MprintStr   = "Macro source (if MPRINT turned on)"
    ;
  run ;

  %if %length(&ProcOutDs.) %then %do ;

    proc means data=&outds. nway noprint ;
      class procedure ;
      var real_hours cpu_hours ;
      output out=&ProcOutDs. (drop=_type_ rename=(_freq_=NumProcCalls)) sum= ;
    run ;

    proc sort data=&ProcOutDs. ; by DESCENDING real_Hours ; run ;

    data &ProcOutDs. ;
      set &ProcOutDs. ;
      retain cumPct 0 ;
      if real_Hours > 0 then pctCPUTime = cpu_Hours / real_Hours ;
      if (&totalTime. > 0) then pctTime = real_Hours / &TotalTime. ;
      cumPct = cumPct + pctTime ;
      format pctCPUTime pctTime cumPct percent8.1 cpu_hours real_hours comma8.2  ;
      label real_hours  = "Total Time in Hours"
            cpu_hours   = "CPU Time in Hours"
            pctCPUTime  = "Pct of total data step/proc time taken by CPU"
            pctTime     = "Pct of ALL data step/proc time"
            cumPct      = "Cumulative Pct of ALL data step/proc time"
      ;
    run ;

  %end ;

  %FINISH:

%mend ;
