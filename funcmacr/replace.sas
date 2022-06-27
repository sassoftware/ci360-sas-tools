/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : Replace.sas
/ Author    : Noah Powers
/ Created   : 2019
/ Purpose   : Function-style macro to replace all occurences of a sub-string with another 
/             sub-string in a user supplied string.  This is accomplished by using the
/             SAS datastep function TRANWRD that is called using the %sysfunc macro 
/             function.
/ FuncOutput: 
/ Usage     : %put %replace(one two three four,two,one)
/ Notes     : This macro does NOT do a search and replace on words.
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/ source            The source string on which the find replace operation is conducted
/ target            The target string to look for in source
/ replacement       The replacement string
/============================================================================================*/
%macro replace(source,target,replacement);

  %if %length(&target.) %then %do ;  
    %sysfunc(tranwrd(&source.,&target.,&replacement.)) 
  %end ;
  %else %do ;
    &source.
  %end ;
  
%mend;
