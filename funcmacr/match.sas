/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : Match.sas
/ Author    : Noah Powers 
/ Created   : 2019
/ Purpose   : Function-style macro to return elements of a list that match those
/             in a reference list.
/ Usage     : %let match=%match(aa bb,aa cc)
/ Notes     : Non-matching list elements are returned in the global macro
/             variable _nomatch_ .
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/ ref              (pos) Space-delimited reference list
/ list             (pos) Space-delimited list
/ nodup=Y          By default, remove duplicates from the list
/ casesens=N       By default, case sensitivity is not important.
/ fixcase=N        By default, do not make the case of matching items the same
/                   as the item in the reference list.
/============================================================================================*/
%macro match(ref,list,nodup=Y,casesens=N,fixcase=N);

  %local error list2 numRefWords numListWords i j item matchFlag refitem;
  %let error=0;

  %global _nomatch_;
  %let _nomatch_=;

  %if not %length(&nodup.) %then %let nodup = Y;
  %if not %length(&casesens.) %then %let casesens = N;
  %if not %length(&fixcase.) %then %let fixcase = N ;

  %let nodup = %upcase(%substr(&nodup.,1,1));
  %let casesens = %upcase(%substr(&casesens.,1,1));
  %let fixcase = %upcase(%substr(&fixcase.,1,1));

  %if ("&nodup" = "Y") %then %let list2 = %nodup(&list.,casesens=&casesens.);
  %else %let list2 = &list. ;

  %let numRefWords = %words(&ref.);
  %let numListWords = %words(&list2.);

  %if (&numRefWords. <= 0) %then %do;
    %put %upcase(Error): (match) No elements in ref macro parameter;
    %let error = 1;
  %end;

  %if (&numListWords. <= 0) %then %do;
    %put %upcase(Error): (match) No elements in list macro parameter ;
    %let error = 1;
  %end;

  %if (&error.) %then %goto error;

  %do i = 1 %to &numListWords. ;
    %let item = %scan(&list2.,&i.,%str( ));
    %let matchFlag = N ;
    %do j = 1 %to &numRefWords. ;
      %let refitem = %scan(&ref.,&j.,%str( ));
      %if (("&casesens." = "N") AND ("%upcase(&item.)" = "%upcase(&refitem.)")) OR ("&item." = "&refitem.") %then %do;
        %let matchFlag = Y ;
        %let j = &numRefWords. ;        
      %end;
    %end;
    %if (&matchFlag. = Y) %then %do ;
      %if ("&fixcase." = "N") %then &item.;
      %else &refitem. ;
    %end ;
    %else 
      %let _nomatch_ = &_nomatch_. &item. ;
  %end;

  %error:
%mend;
