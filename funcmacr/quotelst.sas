/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : QuoteLst.sas
/ Author    : Noah Powers
/ Created   : 2019
/ Purpose   : This is useful to turn a list into a quoted list so that you can
/             use the in() function on it in a data step. 
/ FuncOutput:
/ Usage     : data mydata 
/               set mydata0 (where=(region in (%quotelst(&regionlist.)))
/               ...
/             run 
/               
/ Notes     : 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/ string            String to quote elements of (pos)
/ stringDlm         default is %str( ) - This is the delimiter on the input string
/ quote=%str(%")    Quote character to use (defaults to double quotation mark)
/ delim=%str( )     Delimiter character to use on the output string (defaults to a space).
/                   This code assumes that the delimiter of the input string (str)
/                   is a space.
/============================================================================================*/
%macro quotelst(string,stringDlm=%str( ),quote=%str(%"),delim=%str( ));

  %local i quotelst;

  %let i = 1;

  %do %while(%length(%qscan(&string.,&i.,&stringDlm.)) > 0);
    %if (%length(&quotelst.) = 0) %then 
      %let quotelst = &quote.%qscan(&string.,&i.,&stringDlm.)&quote. ;
    %else 
      %let quotelst = &quotelst.&quote.%qscan(&string.,&i.,&stringDlm.)&quote.;
    %let i = %eval(&i. + 1);
    %if %length(%qscan(&string.,&i.,&stringDlm.)) > 0 %then 
      %let quotelst = &quotelst.&delim. ;
  %end;
  
  %unquote(&quotelst.)

%mend;
