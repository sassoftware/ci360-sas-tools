/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : chk_var_levels.sas
/ Author    : Noah Powers
/ Created   : October 2022
/ Purpose   : This macro will check to see if the user specified list of variables have the 
/             same values for each combination of sortVars. Typically visual investigation of
/             a data table leads to hypothesis about how certain metrics are repeated in
/             the records of a dataset.  This macro is designed to test that hypothesis. 
/             
/ FuncOutput: NA
/ Usage     : 
/ Notes     : 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ inds           The name of the SAS input dataset to be investigated
/ SortVars       The list of primary key variables to use for sorting the input data
/ IgnoreVars     The list of variables NOT to check for same values within a by group
/ ChkIfRepeatedVars The list of variables that are hypothesized to have the same value 
/                   for each by group
/ outds          The name of the output SAS dataset to create with any differences found
/ NumDiffTol     (default 10E-4) If numeric variables differences are greater than this 
/                 - a difference is detected
/============================================================================================*/

%macro chk_var_levels(inds              =,                      
                      SortVars          =,
                      IgnoreVars        =,
                      ChkIfRepeatedVars =,
                      outds             =,
                      NumDiffTol        =10E-4
                      ) ;

  %local OutDsLib OutDsName inViya NumVars i var charvarlist numVarlist cntNumVars cntCharVars 
         maxcharlen NumSortVars lastSortVar ;
  
  %let OutDsLib  = %scan(&OutDs.,1,%str(.)) ;
  %let OutDsName = %scan(&OutDs.,2,%str(.)) ;

  %if not %length(&OutDsName.) %then %do ;
    %let OutDsLib  = WORK ;
    %let OutDsName = &OutDs. ;
  %end ;        
  
  %if ("%substr(&sysvlong.,1,1)" = "V") %then %do ;
    proc contents data=&inds. out=_conts_ noprint ;
    run ;
    
    proc sql noprint ;
      select distinct engine into: libEngine separated by "*" from _conts_ ;
    quit ;
    
    %if "%upcase(&libEngine.)" = "CAS" %then %let inViya = 1 ;      
      %else %let inViya = 0 ;
  %end ;
  %else %let inViya = 0 ;
  
  %if not %length(&ChkIfRepeatedVars.) %then %do ;
    proc contents data=&inds noprint out=varlist (keep=name) ;
    run ;
    
    proc sql noprint ;
      select distinct name into: ChkIfRepeatedVars separated by " " 
        from varlist (where=(upcase(name) not in (%quotelst(%upcase(&SortVars. &IgnoreVars.))))) ;
    quit ;
  %end ;
                      
  %let NumVars = %words(&ChkIfRepeatedVars.) ;
  %let maxcharlen = 0 ;
  %do i = 1 %to &NumVars. ;
    %let var = %scan(&ChkIfRepeatedVars.,&i.,%str( )) ;
    %if ("%vartype(&inds.,&Var.)" = "C") %then %do ;
      %let charvarlist = &charvarlist. &var. ;
      %let maxcharlen = %sysfunc(max(&maxcharlen.,%varlen(&inds.,&var.,IncludeDollarSign=N))) ;
    %end ;
    %else %do ;
      %let numVarlist = &numvarlist. &var. ;
    %end ;
  %end ;
  
  %let cntNumVars = %words(&numvarlist.) ;
  %let cntCharVars = %words(&charvarlist.) ;
  
  %let NumSortVars = %words(&SortVars.) ;
  %do i = 1 %to &NumSortVars. ;
    %let Nextvar = %scan(&SortVars.,%eval(&i.+1),%str( )) ;
    %if "&NextVar." = "" %then %let lastSortVar = %scan(&SortVars.,%eval(&i),%str( )) ;
  %end ;
  
  %if (&inViya. = 0) %then %do ;
    proc sort data=&inds. ; by &SortVars. ; run ;
  %end ;
  
  %if %eval(&cntNumVars.) > 0 %then %do ;
    data numvar2check ;
      length varName $32. ;
      %do i = 1 %to &cntNumVars. ;
        %let var = %scan(&numvarlist.,&i.,%str( )) ;
        varname = "&var." ;
        output ;
      %end ;
    run ;
  %end ;
  
  %if %eval(&cntCharVars.) > 0 %then %do ;
    data charvar2check ;
      length varName $32. ;
      %do i = 1 %to &cntCharVars. ;
        %let var = %scan(&charvarlist.,&i.,%str( )) ;
        varname = "&var." ;
        output ;
      %end ;
    run ;
  %end ;
  
  data &outdsLib.._diffs_ ;
    set &inds. ;
    by &SortVars. ;
        
    %if %eval(&cntNumVars.) > 0 %then %do ; 
      length lastnumdiff n 8. ;
      array nums (&cntNumVars.) &numvarlist. ;
      array num2chk (&cntNumVars.) _TEMPORARY_ ;
      retain num2chk ;      
    %end ;
    
    %if %eval(&cntCharVars.) > 0 %then %do ; 
      length lastchardiff c 8. ;
      array chars (&cntCharVars.) $&maxcharlen. &charvarlist. ;
      array char2chk (&cntCharVars.) $&maxcharlen. _TEMPORARY_ ;         
      retain char2chk ;    
    %end ;
    
    if first.&lastSortVar. then do ;
      %if %eval(&cntNumVars.) > 0 %then %do ; 
        do n = 1 to dim(num2chk) ;
          num2chk(n) = nums(n) ;
        end ;
      %end ;
      %if %eval(&cntCharVars.) > 0 %then %do ; 
        do c = 1 to dim(char2chk) ;
          char2chk(c) = chars(c) ;
        end ;
      %end ;
    end ;
    else do ;
      %if %eval(&cntNumVars.) > 0 %then %do ; 
        do n = 1 to dim(num2chk) ;
          if abs(num2chk(n) - nums(n)) > &NumDiffTol. then lastnumdiff = n ;
        end ;
      %end ;
      %if %eval(&cntCharVars.) > 0 %then %do ; 
        do c = 1 to dim(char2chk) ;
          if char2chk(c) NE chars(c) then lastchardiff = c ;
        end ;
      %end ;
    end ;
    
    keep &SortVars. &IgnoreVars. &ChkIfRepeatedVars. ;
    %if %eval(&cntNumVars.) > 0 AND %eval(&cntCharVars.) > 0 %then %do ;
      if (lastnumdiff or lastchardiff) then output ; 
      keep lastnumdiff lastchardiff ;
    %end ;
    %else %if %eval(&cntNumVars.) > 0 %then %do ;
      if (lastnumdiff) then output ;    
      keep lastnumdiff ;
    %end ;
    %else %if %eval(&cntCharVars.) > 0 %then %do ;
      if (lastChardiff) then output ;    
      keep lastchardiff ;
    %end ;    
  run ;
  
  proc sort NODUPKEY data=&outdsLib.._diffs_ out=&outdsLib.._diffs_uniq_vals ; by  &SortVars. ; run ;
  
  data &outds. (keep=&SortVars. &IgnoreVars. &ChkIfRepeatedVars.);
    merge &outdsLib.._diffs_uniq_vals (in=inbad keep=&SortVars.)
          &inds.                      (in=inds)
    ;
    by &SortVars. ;
    if inds and inbad ;
  run ;
  
%mend ;