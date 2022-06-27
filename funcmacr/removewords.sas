/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : RemoveWords.sas
/ Author    : Noah Powers
/ Created   : 2019
/ Purpose   : Function-style macro that removes a user specified list of words from
/             a space delimited list of words
/ Usage     : %let VarsToProcess = %removeWords(%varlist(&inds.),&VarsToExclude) 
/ Notes     : For a word to be removed, the whole word must match. This macro
/             will not remove substrings in the sense that "low" will not be
/             removed from the end of the word "yellow". Multiple occurences of
/             a word will be removed. 
/             - This code could probably be improved by using the indexw data
/               step function instead of the second nested loop.
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/ string            (pos) Unquoted space-delimited source list of words
/ targetwords       (pos) Unquoted space-delimited target word(s) to remove
/ casesens=N        Y/N Whether the search for the target word(s) is case sensitive
/============================================================================================*/
%macro RemoveWords(string,targetwords,casesens=N);

  %local i j result matchFlag NumTargetwords NumStringwords word targWord ;

  %if not %length(&casesens.) %then %let casesens = N ;
  %let casesens = %upcase(%substr(&casesens.,1,1));

  %let NumStringwords = %words(&string.);
  %let NumTargetwords = %words(&targetwords.);

  %do i = 1 %to &NumStringwords. ;
    %let matchFlag = 0 ;
    %let word = %scan(&string.,&i.,%str( ));
    %do j = 1 %to &NumTargetwords. ;
      %let targWord = %scan(&targetwords.,&j.,%str( ));
      %if (("&casesens." EQ "Y") AND ("&targWord." = "&word.")) OR ("%upcase(&targWord.)" = "%upcase(&word.)") %then %do;
        %let matchFlag = 1 ;
        %let j = &NumTargetwords. ;
      %end;      
    %end;
    %if (not &matchFlag.) %then %let result = &result. &word. ;
  %end;

  %if %length(&result.) %then %let result = %sysfunc(compbl(&result.));
  &result.

%mend;
