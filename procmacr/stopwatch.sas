/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : StopWatch.sas
/ Author    : Noah Powers
/ Created   : 2019
/ Purpose   : This macro acts like a stop watch for measuring the time for pieces of SAS code
/             to complete.  When this macro is invoked with the start action it stores the 
/             current date/time in a global macro variable and optionally prints messages 
/             out to a user specified location(s).  When this macro is invoked with the 
/             stop action it calculates the elapsed time between when it was started and 
/             stopped and optionally prints messages out to user specified locations(s).
/ FuncOutput: NA
/ Usage     :
/ Notes     : 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ Action            (start/stop) This is a positional parameter with two possible
/                    values: start or stop.  These work as described in the purpose
/                    section above.
/ GlobVarName       This is the name of the global macro variable to store the
/                   start time in.  It is user customizable so that if the default
/                   is already being used it can be changed.  This should not be
/                   changed between the start and stop invocations of the macro
/                   as this will cause the macro to work incorrectly.  The default 
/                   value is _StartTime_.
/ OutFilenames      (optional) If this is provided, it is expected to be a
/                   space delimited list of valid FILENAME references to which
/                   text messages will be written.  The default value is LOG.
/ Banner            (optional) If this is provided, it is expected to be a
/                   text string that will be printed out as the first line of
/                   output to the OutFiles.  This could be something like:
/                   -- Starting process mySAS for client xyz ---
/ AddlText          (optional) If this is provided, it is expected to be a text
/                   string that will be printed to the Outfiles after the banner
/                   and after the time information.
/============================================================================================*/
%macro StopWatch(action,
                 GlobVarName=_StartTime_,
                 OutFilenames=LOG,
                 Banner=,
                 AddlText=) ;

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

  %local MacroName i outfile ;
  %let MacroName = &sysmacroname. ;

  ***----------------------------------------------------***;
  ***                  Validate the inputs               ***;
  ***----------------------------------------------------***;

  %let action = %upcase(&action.) ;
  %if ("&action." ne "START") and ("&action." ne "STOP") %then %do ;
    %put %cmpres(E%upcase(rror): (&MacroName.) Invalid value provided for ACTION (&action.).  
         Valid values are: start stop.) ;
    %goto FINISH ;
  %end ;

  %if not %length(&GlobVarName.) %then %let GlobVarName = _StartTime_ ;
  %global &GlobVarName. ;

  ***----------------------------------------------------***;
  ***                    Main Body                       ***;
  ***----------------------------------------------------***;

  %if ("&action." = "START") %then %let &GlobVarName. = %sysfunc(datetime()) ;

  %if %length(&outfileNames.) %then %do ;
    data _null_ ;
      start = &&&GlobVarName. ;
      %do i = 1 %to %words(&outfileNames.) ;
        %let outfile = %scan(&outfileNames.,&i.,%str( )) ;
        FILE &outfile. ;
        put "----------------------------------------------------" ;
        %if %length(&banner.) %then %do ;
          put "-- %cmpres(&banner.)" ;
        %end ;
        %if ("&action." = "START") %then %do ;
          put "-- Start time: " start dateampm20. ;
        %end ;
        %else %if ("&action." = "STOP") %then %do ;
          stop = datetime() ;
          ElapsedTime = stop - start ;
          put "-- Start time: " start dateampm20. ;
          put "-- Stop time:  " stop dateampm20. ;
          put "-- Elapsed time: " ElapsedTime time10.2 ;
        %end ;
        %if %length(&AddlText.) %then %do ;
          put "-- %cmpres(&AddlText.)" ;
        %end ;
        put "----------------------------------------------------" ;
      %end ;
    run ;
  %end ;

  %FINISH:

%mend ;
