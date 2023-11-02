/*
/===========================================================================================
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : Check_Log.sas
/ Author    : Noah Powers
/ Created   : 2019
/ Purpose   : To read a SAS log into a SAS dataset and scan the text to identify errors, 
/             warnings, notes that indicate (or may indicate) that something went wrong 
/             in the SAS code.  Warnings from the SAS license being expired are ignored.  
/             This macro can be run against the SAS log window in interactive sas or 
/             against a saved ASCII text file.
/ FuncOutput: N/A
/ Usage     : need to update this
/ Notes     : - Need to make sure that all relevant issues are getting trapped for
/             - Do we need additional levels of error categories?
/             - Could enable the user to specify the errors/issues that should be flagged
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ filepath          (optional) The full file path and name of the SAS log file to be scanned.
/                   If this parameter is left NULL then this macro will check the text in the 
/                   SAS log window. 
/ LinesToPrint      If PRINT is set to Y the value of this parameter determines the maximum 
/                   number of suspicious lines in the log to print to the SAS listing.
/ Print             (Y/N) If this is set to Y then the first LINESTOPRINT (or less) are 
/                   printed to the SAS listing file/window.  The default value is Y.
/ Outds             (optional) This is the name of the SAS dataset that is created that will
/                   contain any suspect lines in the log.  The default name is _Check_Log_.
/ Run_Time_Check    (optional) This is the name of the SAS dataset that is created that will be 
/                   the result of running the log thru %log_run_time (OutDs)
/ Run_Time_Summary  (optional) This is the name of the SAS dataset that is created that will contain
/                   the resulting time summary by proc generated from the %log_run_time (ProcOutDs)
/============================================================================================*/
%macro Check_Log(filepath               = ,
                 LinesToPrint           = 20,
                 print                  = Y,
                 outds                  = _Check_Log_ ,
                 Run_Time_Check         = ,
                 Run_Time_Summary       = 
                 ) ;         

  %local MacroName NumLines mprintOption xfmterr ;
  %let MacroName = &sysmacroname.;

  %Print_Macro_Parameters( &MacroName. ) ;
  
  %let mprintOption = %sysfunc(getoption(MPRINT));
  %let xfmterr = %sysfunc(getoption(fmterr)) ;

  options nofmterr nomprint ;

  **--------------------------------------------------**;
  **     Validate user inputs passed to macro         **;
  **--------------------------------------------------**; 

  %if not %length(&print.) %then %let print = Y ;
  %let print = %upcase(%substr(&print.,1,1)) ;
  
  proc format ;
    value $errLvl 
    "1" = "1 - Serious Error"
    "2" = "2 - Possible Error"
    "3" = "3 - Low Issue"
    "4" = "4 - MO tuning"
    ;
  run ;

  filename temp_log "&filepath." ;

  data &outds. ;
    length logline logline2 $700. errLevel $1. errLevelDesc $25. ;
    infile temp_log truncover LRECL=32000 end=lastrec length=len ;
    input @1 logline & $varying700. len ;
    lineNum = _N_ ;
    logline2 = upcase(logline);
    if (index(logline2, "NOTE:")) & (index(logline2, " MISSING ") | index(logline2, " ILLEGAL ") |
        index(logline2, " INVALID ") | index(logline2, " NOT EXIST ") | 
        index(logline2,"FORMAT WAS TOO SMALL FOR THE NUMBER TO BE PRINTED")) and
        logline2 ne "NOTE: BASE DATA SET DOES NOT EXIST. DATA FILE IS BEING COPIED TO BASE FILE." 
    then do ;
      errLevel = "3" ;
      ErrLevelDesc = put(errLevel,$errLvl.) ;
      output;
    end ;
    else if (index(logline2,"INFO:") & index(logline2,"WILL BE OVERWRITTEN BY"))
    then do ;
      ErrLevel = "2" ;
      ErrLevelDesc = put(errLevel,$errLvl.) ;
      output ;
    end ;
    else if logline2 =: "WARNING:" and 
      index(logline2,"WILL BE EXPIRING SOON") or 
      index(logline2,"THE BASE PRODUCT PRODUCT WITH WHICH") or 
      index(logline2,"YOUR SYSTEM IS SCHEDULED TO EXPIRE ON") or
      index(logline2,"UNABLE TO COPY SASUSER REGISTRY TO WORK REGISTRY")  
    then ;
    else if logline2 =: "ERROR:" or index(logline2,"_ERROR_") or index(logline2,"INVALID COMPRESSED DATA") then do ;
      ErrLevel = "1" ;
      ErrLevelDesc = put(errLevel,$errLvl.) ;
      output ;
    end ;
    else if index(logline2,"WARNING:") & index(logline2,"THE ID COLUMNS WERE TOO WIDE; SOME WERE TRANSFORMED") 
    then do ;
      ErrLevel = "3" ;
      ErrLevelDesc = put(errLevel,$errLvl.) ;
      output ;
    end ;
    else if index(logline2,"WARNING:") |
      index(logline2,"UNINITIAL")  | index(logline2,"NOT RESOLVE") |
      index(logline2,"AT LEAST")   | index(logline2,"NOT IN DATA") |
      index(logline2,"NO INPUT")   | index(logline2,"INSUFFICIENT") |
      index(logline2,"NOT EVALUATED") | index(logline2,"REPEATS OF BY VALUES")
    then do ;
      ErrLevel = "2" ;
      ErrLevelDesc = put(errLevel,$errLvl.) ;
      output;
    end ;
    else if index(logline2,"BEST INSTANCE (#") then do ;
      ErrLevel = "4" ;
      ErrLevelDesc = put(errLevel,$errLvl.) ;
      output;
    end ;
    drop logline2 ;
  run ;
  
  filename temp_log clear ;

  proc sort data=&outds. ; by ErrLevel lineNum ; run ;

  %let NumLines = %nobs(&outds.) ;
  %if ("&print." = "Y") %then %do ;
    %if &NumLines. > 0 %then %do ;
      title "(&MacroName.) The following &NumLines. suspicious lines were found in the log:" ;
      %if &NumLines. > &LinesToPrint. %then %do ;
        title2 "Note: Only the first &LinesToPrint. of &NumLines. suspicious lines printed" ;
        title3 "Note: See &outds. for full details." ;
      %end ;
      proc print NOOBS heading=H data=&outds. (obs=&LinesToPrint.) ;
        by Errlevel ;
        id Errlevel ErrlevelDesc ;
        var lineNum logline ;
        format logline $150. ;
      run ;
      title ;
      title2 ;
      title3 ;
    %end ;
    %else %do ;
      data _null_ ;
        file PRINT ;
        put ;
        put "%sysfunc(repeat(-, 34 + %length(&sysmacroname.)))" ;
        put "-- (&MacroName.) No issues found in the log --" ;
        put "%sysfunc(repeat(-, 34 + %length(&sysmacroname.)))" ;
        put ;
      run ;
    %end ;
  %end ;

  %if %length(&Run_Time_Check.) %then %do;
        
    %Log_Run_Time(  LogFile   = &filepath. ,
                    OutDs     = &Run_Time_Check. , 
                    ProcOutDs = &Run_Time_Summary.
                    ) ;             
  %end;

  %FINISH:
  options &mprintOption. &xfmterr. ;

%mend ;
