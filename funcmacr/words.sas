/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : Words.sas
/ Author    : Noah Powers 
/ Created   : 2019
/ Purpose   : Function-style macro to return the number of words in a text string.  The
/             words are defined by the delimiter which is a space by default.
/ Usage     : %let Varlist = %varList(inds)
/             %let NumVars = %words(&Varlist)
/             %do i = 1 %to &NumVars.
/               ...
/             %end  
/ Notes     : The delimiter parameter is the list of characters that are used to define
/             the words.  If more than one character is provided - this string is NOT
/             treated as the delimiter. See some examples below:
/             %words(AABBCCDD,delim=%str(AB))
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/ str               (positional) The text string UNQUOTED
/ delim             The delimeter(s) that are used to define the words.  The default value
/                   is just the space character.  
/============================================================================================*/
%macro words(str,delim=%str( ));

  %local words ;
  %let words = 1 ;
  %do %while(%length(%qscan(&str.,&words.,&delim.)) > 0);
    %let words = %eval(&words. + 1);
  %end;
  %eval(&words.-1)

%mend;
