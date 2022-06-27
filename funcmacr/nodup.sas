/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : NoDup.sas
/ Author    : Noah Powers 
/ Created   : 2019
/ Purpose   : Function-style macro to remove duplicates in a space-delimited list
/ Usage     : %let str=%nodup(aaa bbb aaa)
/ Notes     : 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/ list              (positional) space delimited list of items to de-dup
/ casesens          Y/N Flag to make process case sensitive or not
/============================================================================================*/
%macro nodup(list,casesens=N);

  %local i j match item NumItems ;

  %if not %length(&casesens.) %then %let casesens = N ;
  %let casesens = %upcase(%substr(&casesens.,1,1));
  %let NumItems = %words(&list.) ;

  %do i = 1 %to &NumItems. ;
    %let item = %scan(&list.,&i.,%str( ));
    %let match = N ;
    %if (&i. < &NumItems.) %then %do j = %eval(&i. + 1) %to &NumItems. ;
      %if (&casesens. = Y) %then %do;
        %if "&item." = "%scan(&list.,&j.,%str( ))" %then %let match = Y ;
      %end;
      %else %do;
        %if "%upcase(&item.)" = "%upcase(%scan(&list.,&j.,%str( )))" %then %let match = Y ;
      %end;
    %end;
   
    %if (&match. = N) %then &item. ;
  %end;

%mend ;
