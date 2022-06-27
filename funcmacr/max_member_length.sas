/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   :   Max_Member_Length.sas
/ Author    :   Noah Powers
/ Created   :   2019
/ Purpose   :   This macro takes a delimited list and outputs the max length of 
                any given member.  This function does not report what entity has said max length.
/ FuncOutput:   %let max_length = %Max_Member_Length(&ARRAY_LIST.) ;
/ Usage     :
/ Notes     :
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ IN_LIST            (pos) The user specified list of words to modify.  The delimiter that
/                    specifies words is specified in IN_DLM
/ IN_DLM             The delimiter that determines words in the IN_LIST.  The default is
/                    a space %str( )
/============================================================================================*/
%macro Max_Member_Length(IN_LIST ,
                         IN_DLM  = %str( )
                         ) ;

  %local m _DS_ _MAXLENGTH ;                
  %let _MAXLENGTH = 0 ;

  %do m = 1 %to %words(&IN_LIST.,DELIM=&IN_DLM.) ;
    %let _DS_ = %qscan(&IN_LIST.,&m.,&IN_DLM.) ;
    %let _MAXLENGTH = %sysfunc( max(0,&_MAXLENGTH.,%length(&_DS_.))) ;          
  %end;

  &_MAXLENGTH.

%mend;
