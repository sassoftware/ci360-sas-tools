/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : upload_data2hub.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2020 January
/ LastModBy : Noah Powers
/ LastModDt : 1.28.2020
/ Purpose   : Upload a user provided data file and JSON descriptor of the file to the I360 
/             HUB
/ FuncOutput: N/A
/ Usage     :
/ Notes     : 1. Execute POST call with JSON data file descriptor to create data descriptor id
/             2. Execute POST call to filetransferlocation endpoint to get signed URL to 
/                use in next proc http call to upload data file.  Data descriptor NOT used here.
/             3. Execute PROC HTTP call with IN= and method=PUT to upload the data to CI360
/             4. Execute the import process POST call with descriptor ID in URL,
/                and JSON file in body with metadata about import including the signed URL
/                used to upload data.
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ JWT                     Json Web Token based on the tenant id and secret key.  
/                         The %Gen_JWT() macro can create this for you.
/ url_base                 Default is %nrstr(https://design-prod.cidemo.sas.com) - change as 
/                          appropriate for your tenant
/ DataDescriptorID       = If you have already created the data descriptor ID, provide the
/                          value here and leave DataDescriptorJsonFile blank.
/ DataDescriptorJsonFile = IF you do NOT have a data descriptor ID, provide the JSON
/                          file (full path and name) with the metadata information as
/                          required by 360 platform.  This parameter is ignored if a
/                          non-blank value for DataDescriptorID is provided.
/ File2Import            = The full file path and name for the file to import
/ fieldDelimiter         =%str(,) Must be comma. Make sure your file is comma delimited
/ fileType               =%str(CSV).  Must be CSV.
/ headerRowIncluded      =%str(true). If no header row is in the file, set this to false
/ updateMode             = This can be either replace or upsert.  See user manual for the 
/                          details on what happens with each value.
/ contentName            = Provide a description of the data file here.
/============================================================================================*/
%macro upload_data2hub(jwt                    =,
                       url_base               =%nrstr(https://extapigwservice-demo.cidemo.sas.com),
                       DataDescriptorID       =,
                       DataDescriptorJsonFile =,
                       File2Import            =,
                       fieldDelimiter         =%str(,),
                       fileType               =%str(CSV),
                       headerRowIncluded      =%str(true),
                       updateMode             =,
                       contentName            =) ;
                  
   
  %local url done slash ;

  %if (&sysscp. = WIN) %then %do;
    %let slash = %str(\);
  %end;
  %else %do;
    %let slash = %str(/);
  %end ;

  **************************************************************************************************;  
  ** 1. Execute POST call with JSON data file descriptor to create data descriptor id   ************;
  **************************************************************************************************;

  %if NOT (%length(%trim(&DataDescriptorID.)) > 0) %then %do ;
  
    %let url = %str(&url_base./marketingData/tables) ;
    filename desc "&DataDescriptorJsonFile." ;
    filename _json_ TEMP ;
    
    %Call_Proc_HTTP(url            =&url.,
                    Method         =POST,
                    jwt            =%superq(jwt),
                    InFile         =desc,
                    jsonOutFileNm  =_json_) ;

    libname jsondata json fileref=_json_ ; 

    data DataId ;
      set jsondata.root ;
      call symputx("DataDescriptorID",id) ;
      put DataDescriptorID= ;
    run ;

    libname jsondata CLEAR ;
    
  %end ;

  **************************************************************************************************;  
  ** 2. Execute POST call to filetransferlocation endpoint to get signed URL to              *******;
  **    use in next proc http call to upload data file.  Data descriptor NOT used here.      *******;
  **************************************************************************************************;  

  %let url = %str(&url_base./marketingData/fileTransferLocation) ;

  filename _json_ TEMP ;
    
  %Call_Proc_HTTP(url            =&url.,
                  Method         =POST,
                  jwt            =%superq(jwt),
                  jsonOutFileNm  =_json_) ;

  libname jsondata json fileref=_json_ ;

  data _null_ ;
    set jsondata.root ;
    call symputx("signedURL",signedURL) ;
    file LOG ;
    put createdTimeStamp= expiresTimeStamp= signedURL= ;
  run ;
  
  libname jsondata CLEAR ;

  **************************************************************************************************;  
  ** 3. Execute PROC HTTP call with IN= and method=PUT to upload the data to CI360              ****;
  **************************************************************************************************;
 
  filename _in_ "&File2Import." ;
  filename _json_ TEMP ;

  %Call_Proc_HTTP(url           =%superq(signedurl),
                 Method         =PUT,
                 InFile         =_in_,                  
                 jsonOutFileNm  =_json_) ;

  ** A return code of 200 is the only indication of success we will get **;

  **************************************************************************************************;   
  ** 4. Execute the import process POST call with descriptor ID in                               ***;
  **    JSON file body with metadata about import including the signed URL                       ***;
  **    used to upload data.                                                                     ***;
  **************************************************************************************************;  

  filename json_imp TEMP ;

  data _null_ ;
    file json_imp LRECL=20000 ;
    put "{" ;
    put '"contentName": "' "&contentName." '",' ;
    put '"dataDescriptorId": "' "&DataDescriptorID." '",' ;
    put '"fieldDelimiter": "' "&fieldDelimiter." '",' ;
    put '"fileLocation": "' "%superq(signedurl)" '",' ;
    put '"fileType": "' "&fileType." '",' ;
    put '"headerRowIncluded": ' "&headerRowIncluded., " ;
    put '"recordLimit": 0,' ;
    put '"updateMode": "' "&updateMode." '"' ;
    put "}" ;
  run ;

  %let url = %str(&url_base./marketingData/importRequestJobs) ;
  
  ** This often fails at the first attempt with not authorized error but should eventually work**;
  %Call_Proc_HTTP(url                     =&url.,
                  Method                  =POST,
                  InFile                  =json_imp,   
                  jwt                     =%superq(jwt),                 
                  http_retry_wait_sec     =60,
                  jsonOutFileNm           =_json_) ;

  libname jsondata json fileref=_json_ ;

  data ImportMeta ;
    set jsondata.root ;
    call symputx("ImportRequestID",id) ;
    put id= status= statusDescription=;
  run ;

  libname jsondata CLEAR ;

  **************************************************************************************************;  
  ** 5. Check on status of the import                                                           ****;
  **************************************************************************************************;

  %let url = %str(&url_base./marketingData/importRequestJobs/&importRequestID.) ;
  %let done = 0 ;

  %do %while (NOT &done.) ;

    filename _json_ TEMP ;

    %Call_Proc_HTTP(url            =&url.,
                    Method         =GET,
                    jwt            =%superq(jwt),   
                    jsonOutFileNm  =_json_) ;

    libname jsondata json fileref=_json_ ;

    data ImportStatus ;
      set jsondata.root ;
      file log ;
      put "Import Status: " status ;
      x = sleep(30,1) ;
      if upcase(status) in ("IMPORTED" "FAILED VALIDATION") then 
        call symputx("done","1") ;
      drop x ;
    run ;

  %end ;

  data ImportReport ;
    set jsondata.alldata ;
  run ;

  title "Import Parameters" ;
  proc print data=ImportReport ;
    where P=1 AND p1 NE "" And v = 1;
    var p1 value ;
  run ;
  
    title "Status Messages by step" ;
  proc print data=ImportReport ;
    where P=2 AND p1 = "statusInfo" And v = 1 ;
    var p2 value ;
  run ;

  title "Detailed Status Messages by step" ;
  proc print data=ImportReport ;
    where P = 3 and p2 = "statusMessages" and v = 1 ;
    var P3 value ;
  run ;

  /*
  proc print data=jsondata.messages_info ;
  run ;

  proc print data=jsondata.messages_info2 ;
  run ;
  
  proc print data=jsondata.STATUSINFO_DATAPROCESSING ;
  run ;

  proc print data=jsondata.STATUSINFO_IDENTITYPROCESSING ;
  run ;

  proc print data=jsondata.STATUSINFO_IMPORTVALIDATION ;
  run ;

  proc print data=jsondata.STATUSINFO_INPUTFILEPROCESSING ;
  run ;
  */


  %FINISH:
%mend ;
                       