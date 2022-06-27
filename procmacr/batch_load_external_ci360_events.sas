/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Batch_load_external_ci360_events.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2020 January
/ LastModBy : Noah Powers
/ LastModDt : 1.28.2020
/ Purpose   : Upload a user provided data file containing customer records and appropriate
/             attributes for an External CI360 Event 
/ FuncOutput: N/A
/ Usage     :
/ Notes     : 1. Execute authenticated POST call with JSON to specify bulk event upload.  T
/             2. Execute PUT call to signed URL from previous step with IN= to upload the data to CI360
/             
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ JWT                     Json Web Token based on the tenant id and secret key.  
/                         The %Gen_JWT() macro can create this for you.
/ ExtGatewayAddress       The external gateway address for the tenant.  This can be found in the 
/                         CI360 UI at General | External | Access. 
/ File2Import             The full file path and name for the file to import.  This file must be
/                         comma delimited values and contain a header row
/============================================================================================*/
%macro batch_load_external_ci360_events(jwt                    =,
                                        ExtGatewayAddress      =,                                       
                                        File2Import            =) ;
                  
  %local url done slash signedURL ;

  %if (&sysscp. = WIN) %then %do;
    %let slash = %str(\);
  %end;
  %else %do;
    %let slash = %str(/);
  %end ;

  **************************************************************************************************;  
  ** 1. Execute POST call with JSON event generator file to get a signed URL for next step *********;
  **************************************************************************************************;

  filename json_imp TEMP ;

  data _null_ ;
    file json_imp LRECL=20000 ;
    put "{" ;
    put '"version":1 ,' ;
    put '"applicationId":"eventGenerator"'  ;
    put "}" ;
  run ;
  
  %let url = %str(&ExtGatewayAddress./marketingGateway/bulkEventsFileLocation) ;
  filename _json_ TEMP ;
    
  %Call_Proc_HTTP(url            =&url.,             
                  Method         =POST,
                  jwt            =%superq(jwt),
                  InFile         =json_imp,
                  jsonOutFileNm  =_json_) ;

  libname jsondata json fileref=_json_ ; 

  data DataId ;
    set jsondata.links ;
    call symput("signedURL",strip(href)) ;
    put href= ;
  run ;

  libname jsondata CLEAR ;
  
  **************************************************************************************************;  
  ** 3. Execute PROC HTTP call with IN= and method=PUT to upload the data to CI360              ****;
  **************************************************************************************************;
 
  filename _in_ "&File2Import." ;
  filename _json_ TEMP ;

  %Call_Proc_HTTP(url           =%superq(signedurl),
                 Method         =PUT,
                 HeaderList     =%str("content-type" = "application/octet-stream"),
                 InFile         =_in_,                  
                 jsonOutFileNm  =_json_) ;

  ** A return code of 200 is the only indication of success we will get **;

  %FINISH:
%mend ;                  