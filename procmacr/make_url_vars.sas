/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Make_URL_Vars.sas
/ Author    : Noah Powers
/ Created   : 2020
/ Purpose   : This macro creates new columns from a source URL variable.  The following
/             new columns are written to OUTDS along with all of the original columns
/             found in INDS.  The intent of these variables are to help group and 
/             organize many URLs into similar groups:
/
/             - QUERY_STRING : This is everything to the right of the ? 
/             - URL_FRAG     : This is the URL fragment (after # but before ?)
/             - URL_TAIL     : This is everything after the first slash after
/                              the query string and fragment have been removed
/             - URLLEVEL1-URLLEVEL<max> : These columns are the individual level
/               values found in the URL_TAIL
/             
/ FuncOutput: NA
/ Usage     : 
/ Notes     : 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ inds          The name of the SAS inptut dataset that contains the URLVAR to be processed
/ URLVar        The name of the column in INDS that contains URL values to process
/ URLLevelLen   (default = 200) The length to use for each of the the URLLEVEL1- URLLEVEL<max> 
/               columns created in the OUTDS
/ outds         The name of the output SAS dataset to create 
/ outdsOpts     (default compress=YES) optional dataset options to apply to the output 
/               dataset created.
/============================================================================================*/
%macro Make_URL_Vars(inds        =,
                     URLVar      =,
                     URLLevelLen =200,
                     outds       =,
                     outdsOpts   =%str(compress=YES)) ;

  %local MaxSlashCnt InDsLib InDsName urllen ;
  
  %let InDsLib  = %scan(&InDs.,1,%str(.)) ;
  %let InDsName = %scan(&InDs.,2,%str(.)) ;

  %if not %length(&InDsName.) %then %do ;
    %let InDsLib  = WORK ;
    %let InDsName = &InDs. ;
  %end ;
  
  %let urlLen = %varlen(&inds.,&URLVar.) ;

  ** determine the max number of levels in the URL **;
  data &indsLib.._temp_ ;
    set &inds. (keep=&URLVar.) end=lastrec ;
    length url_tail &urllen. ;
    *retain MaxSlashCnt 0 ;

    endPos = indexc(&URLVar.,"?#") ;
    if endPos > 0 then 
      url_tail = substr(&URLVar.,1,endpos-1) ;
    else 
      url_tail = strip(&URLVar.) ;

    pos = index(url_tail,scan(url_tail,3,"/")) ;
    if pos = 0 OR pos = length(strip(url_tail)) then 
      url_tail = "" ;
    else 
      url_tail = substr(url_tail,pos) ;
    
    slashcnt = count(url_tail,"/") ;
    *if slashcnt > MaxSlashCnt then MaxSlashCnt = slashCnt ;
    *if lastrec then output ; 
    keep slashcnt ;
  run ;
  
  proc means data=&indsLib.._temp_ nway noprint ;
    var slashcnt ;
    output out=&indsLib..slashMax max(slashcnt)=MaxSlashCnt ;
  run ;
  
  proc sql noprint ;
    select maxslashCnt into: MaxSlashCnt from &indsLib..slashMax  ;
  quit ;
  %let MaxSlashCnt = %trim(&MaxSlashCnt.) ;
  
  proc datasets library=&indsLib. nolist ;
    delete _temp_ slashMax ;
  quit ;

  ** split URL text into multiple parts to assist with page grouping **;
  data &outds. (&outdsOpts.) ;
    set &inds. ;
    length url_tail query_string url_frag %varlen(&inds.,&URLVar.) ;
    array url_lvl (&MaxSlashCnt.) $&URLLevelLen.. urlLevel1-urlLevel&MaxSlashCnt. ;

    pos = index(&URLVar.,scan(&URLVar.,3,"/")) ;
    if pos = 0 OR pos = length(strip(&URLVar.)) then 
      url_tail = "" ;
    else 
      url_tail = substr(&URLVar.,pos) ; 
  
    if url_tail > " " then do ;
      query_str_pos = index(url_tail,"?") ;
      if query_str_pos > 0 then do ;
        query_string = substr(url_tail,query_str_pos+1) ;
        if query_str_pos > 1 then url_tail = substr(url_tail,1,query_str_pos-1) ;
          else url_tail = "" ;
      end ;
      hash_tag_pos = index(url_tail,"#") ;
      if hash_tag_pos > 0 then do ;
        url_frag = substr(url_tail,hash_tag_pos+1) ;
          if hash_tag_pos > 1 then url_tail = substr(url_tail,1,hash_tag_pos-1) ;
        else url_tail = "" ;
      end ;
      if url_tail in ( "/" "." " " "-") then url_tail = "" ;
      if substr(url_tail,length(url_tail)) in ("/" ".") then 
        url_tail = substr(url_tail,1,length(url_tail)-1) ;
      if substr(url_tail,1,1) in ("/") then 
        url_tail = substr(url_tail,2) ;
      url_tail = strip(url_tail) ;

      do cnt = 1 to dim(url_lvl) ;
        url_lvl(cnt) = lowcase(scan(url_tail,cnt,"/")) ;
      end ;
    end ;
    *keep &URLVar. url_tail query_string url_frag urlLevel1-urlLevel&MaxSlashCnt. query_str_pos hash_tag_pos cnt pos ;
    drop query_str_pos hash_tag_pos cnt pos ;
  run ;

  %FINISH: ;
%mend ;