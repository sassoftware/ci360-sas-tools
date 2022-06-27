/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   :   Print_Macro_Parameters
/ Author    :   Noah Powers
/ Created   :   2020
/ Purpose   :   Print local variables (parameters) scoped to said macro
/ FuncOutput:
/ Usage     :   %Print_Macro_Parameters( &MacroName. ) ;
/ Notes     :   Prints list of said macro variables to log.  Useful for debugging purposes
/============================================================================================*/
%macro Print_Macro_Parameters( MacroName ) ;

  %local  xmprint xnotes ;
        
  %let xmprint = %sysfunc(getoption(mprint)) ;
  %let xnotes = %sysfunc(getoption(notes)) ;        

  options nonotes nomprint ;

  proc sql;
    create table WORK.___MACRO_PARAMETERS as select name, value
    from sashelp.vmacro 
    where upcase(scope) = upcase("%superq(MacroName)")
    order by name ;
  quit;
                
  %if &SQLOBS. %then %do;

    data _null_ ;
      set WORK.___MACRO_PARAMETERS end = lastrec ;

      if _N_ = 1 then do;
        put '0d0a'x "%sysfunc(repeat(*, 42 + %length(&MacroName.)))" ;
        put "---- Macro parameter values passed to &MacroName. ----" ;
        put "%sysfunc(repeat(*, 42 + %length(&MacroName.)))" '0d0a'x ;
      end ;

      put "   > " name "= " value ;

      if lastrec then do;
        put ;
        put "%sysfunc(repeat(*, 42 + %length(&MacroName.)))" '0d0a'x ;
      end;
    run ;           
                
  %end;
                
  %else %do;
    %put    ;
    %put    %sysfunc(repeat(*, 43 + %length(&MacroName.))) ;
    %put    %str(---- No local macro variables found in &MacroName. ----) ;
    %put    %sysfunc(repeat(*, 43 + %length(&MacroName.))) ;
    %put    ;
  %end;

  proc datasets library = WORK nolist nowarn ;
    delete  ___MACRO_PARAMETERS
  quit;                    

  options &xnotes. &xmprint. ;

%mend  ;

