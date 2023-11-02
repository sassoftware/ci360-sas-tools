/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Call_Proc_HTTP.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2019 August
/ LastModBy : Noah Powers
/ LastModDt : 11.19.2019
/ Purpose   : Invoke proc http with user specified parameters.  Includes code to retry 
/             proc http if it fails.  Also this code automatically checks output 
/             header for any errs 
/ FuncOutput: N/A
/ Usage     : add some text
/ Notes     : Removed CT option as this has been depreciated according to hte SAS documentation:
/             Beginning with SAS 9.4M3, this option is supported for compatibility with previous 
/             versions of SAS software. Use the HEADERS Statement instead of CT= .  Also,
/             Use the HEADERS statement instead of the PROC HTTP CT= and HEADERIN= arguments.
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ url                     The URL string paramter to be passes to proc http 
/ jwt                     The Java Web Token string which is one authentication method for 360 
/ headerList              (optional) This is the list of quoted name value pairs to be included
/                         in the header.  By default this contains a specification for content-type
/ InFile                  (optional) This is an optional filename reference to a input that is 
/                         provided to proc http with the in= option on the proc http statement.
/                         For example, this is used to send the JSON file with Event 1.0
/                         data requrest to the API.
/ Method                  Default=GET This is the http method to be passed to proc http
/ UserName                Some of the 360 programming interfaces require user level authentication
/ Password                If the user name is provided for authentication then the password
/                         must be supplied as well
/ http_max_retry_attempts Default=3 Maximum number of times to re-try proc http after a failure
/ http_retry_wait_sec     Default=5 How many seconds to wait between retrying
/ jsonOutFileNm           A filename to hold the JSON output from proc http
/ http_debug_level        Default=0 The debug parameter passed to proc http
/ TimeoutSec              Default=60 The max number of seconds to wait for proc http to respond.
/                         The value of 0 means it waits forever.
/ proxy_host              (optional) specifies the Internet host name of an HTTP proxy server.
/ proxy_port              (optional) specifies an HTTP proxy server port.
/ proxy_user              (optional) user name to use with proxy server
/ proxy_pwd               (optional) password to use with proxy server
/============================================================================================*/
%macro Call_Proc_HTTP(url                     =,
                      jwt                     =,
                      headerList              =%str("content-type" = "application/json ; charset=utf-8"),                      
                      InFile                  =,
                      Method                  =GET,
                      UserName                =,
                      Password                =,
                      http_max_retry_attempts =3,
                      http_retry_wait_sec     =5,
                      jsonOutFileNm           =,
                      http_debug_level        =0,
                      TimeoutSec              =60,
                      proxy_host              =,
                      proxy_port              =, 
                      proxy_user              =,
                      proxy_pwd               =
                      );

  %local procHttpAttemptNum done status_code DebugValid ;

  %if NOT %length(&TimeoutSec.) %then %let TimeoutSec = 60 ;

  %*** Remove blanks from URL **;
  %let url = %qtrim(&url.) ;

  %let DebugValid = 1 ;

  data _sas_ ;
    length sysvlong $30. ;
    sysvlong = symget("SYSVLONG");                /* system macro variable */
    
    if substr(sysvlong,1,1) = "V" then do ;    /* VIYA */
      sysvlong = substr(sysvlong,3) ;
      viya = 1 ;

      pos1 = find(sysvlong, "."); 
      major = input(substr(sysvlong, 1, pos1-1),8.);          /* major version */
   
      pos2 = find(sysvlong, "M", 'i', pos1+1);
      minor = input(substr(sysvlong, pos1+1, pos2-pos1-1),8.) ;/* minor version */
    end ;
    else do ;
      pos1 = find(sysvlong, ".");
      major = input(substr(sysvlong, 1, pos1-1),8.);          /* major version */
   
      pos2 = find(sysvlong, ".", 'i', pos1+1);
      minor = input(substr(sysvlong, pos1+1, pos2-pos1-1),8.) ;/* minor version */
     
      pos3 = find(sysvlong, "M", 'i', pos2+1);
      iteration = input(substr(sysvlong, pos2+1, pos3-pos2-1),8.) ;/* iteration version */
     
      pos4 = notdigit(sysvlong, pos3+1);
      maint = input(substr(sysvlong, pos3+1, pos4-pos3-1),8.);   /* maintenance level */
    end ;
    if NOT viya AND (major < 9 or minor < 4 or maint < 5) then call symputx("DebugValid",0) ;
      else if major < 3 or minor < 2 then call symputx("DebugValid",0) ;

    drop pos1-pos4 ;
  run ;

  filename hdrout TEMP ;
  %let done = 0 ;
  %let procHttpAttemptNum = 0 ;

  %do %until (&done.) ;

    %let procHttpAttemptNum = %eval(&procHttpAttemptNum. + 1) ;
    %put Note: HTTP call attempt number: &procHttpAttemptNum. ;

    proc http out=&jsonOutFileNm. timeout=&TimeoutSec. headerout=hdrout HEADEROUT_OVERWRITE 
      %if %length(&InFile.) %then %do ;
        in=&infile.
      %end ;
      %if (%length(%superq(UserName)) > 0 and %length(%superq(password)) > 0) %then %do;
        AUTH_BASIC 
        WEBUSERNAME="%superq(UserName)" 
        WEBPASSWORD="%superq(password)"
      %end;
      %if (%length(&proxy_host.) > 0 and %length(&proxy_port.) > 0) %then %do;
        proxyhost="&proxy_host."
        proxyport=&proxy_port.      
        %if %length(&proxy_user.) AND %length(&proxy_pwd.) %then
        %do;
          PROXYUSERNAME="&proxy_user."
          PROXYPASSWORD="&proxy_pwd."
        %end;
      %end;
      method="%upcase(&method.)" 
      url="%superq(url)" CLEAR_CACHE ;
      %if %length(&jwt.) OR %length(&headerList.) %then %do ;
        headers 
      %end ;
      %if %length(&jwt.) %then %do ;
        "Authorization" = "Bearer &jwt." 
      %end ;
      %if %length(&headerList.) %then %do ;
        &headerList. 
      %end ;
      ;
      %if (&DebugValid.) %then %do ;
        DEBUG level=&http_debug_level. ;
      %end ;
    run;

    %* Check proc http execution status ;
    %if &SYSERR. > 4 %then %do;
      %put E%upcase(rror): Executing proc http call (&SYSERRORTEXT.) ;
      %if &procHttpAttemptNum. < &http_max_retry_attempts. %then %do ;
        data _null_ ;
          call sleep(&http_retry_wait_sec.,1);
        run;
      %end ;
      %else %do ;
        done = 1 ;
      %end ;
    %end;
    %else %do ;
      ** Check output header file and print to log **;
      data _null_ ;
        length status_code $20. Line $4000; 
        infile hdrout length=reclen end=lastrec ; 
        input Line $varying999. reclen ;
        retain status_code ;
        file log ;
        put line ;
        if (_N_ = 1) AND (kindex(upcase(Line),'HTTP/1.1')) then do ;
          status_code = kscan(Line, 4);  
          call symput("status_code",trim(left(status_code))) ;
        end ;
        if lastrec AND (trim(left(status_code)) not IN ("200" "201")) then do ;
          if trim(left(status_code)) NE "201" then 
            put "W%upcase(arning): status code (" status_code +(-1) ") from API call is not 200/201" ;
          else 
            put "W%upcase(arning): status code (" status_code +(-1) ") from API call is 201 (depreciated call)" ;
        end ;
      run;

      %if ("&status_code." NE "200") AND ("&status_code." NE "201")
          AND (&procHttpAttemptNum. < &http_max_retry_attempts.) %then %do ;
        data _null_;
          call sleep(&http_retry_wait_sec.,1);
          infile &jsonOutFileNm. length=reclen end=lastrec ; 
          input Line $varying999. reclen ;
          file log ;
          if _N_ = 1 then put "JSON Out text:" ;
          put line ;
          if _N_ > 20 then stop ;
        run;
      %end ;
      %else 
        %let done = 1 ;
    %end ;

  %end ;

  %FINISH:
%mend ;

