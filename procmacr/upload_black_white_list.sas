/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Upload_Black_White_List.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2020 May
/ LastModBy : Noah Powers
/ LastModDt : 05.27.2020
/ Purpose   : Upload a white list or black list to be applied to all recommendation
/             tasks on a tenant.  
/ FuncOutput: N/A
/ Usage     :
/ Notes     : https://blogs.sas.com/content/sasdummy/2015/04/16/how-to-convert-a-unix-datetime-to-a-sas-datetime/
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ GatewayServer           The <server> part of the gateway address where the gateway structure is:
/                         https://extapigwservice-<server>/marketingGateway
/ Tenant_ID               The tenant ID 
/ Secret                  The secret key associated with the Tenant ID.  Together these will be 
/                         used to create the JWT
/ DescriptorFile          The full file path and name of the the JSON descriptor file associated
/                         with the csv
/ CSVListFile             The full file path and name of the CSV (white list or black list) file
/                         to be uploaded and applied to recommendations
/============================================================================================*/
%macro Upload_Black_White_List(GatewayServer  =,
                               tenant_id      =,
                               secret         =,
                               DescriptorFile =,
                               CSVListFile    =);

  %local DSC_AUTH_TOKEN secureURL TransferID EndPoint done slash ;

  %if (&sysscp. = WIN) %then %do;
    %let slash = %str(\);
  %end;
  %else %do;
    %let slash = %str(/);
  %end ;

  %let EndPoint = https://extapigwservice-&GatewayServer./marketingData/analytic/transfers ;

  ** Generate JWT with provided macro or via other method **;
  %let DSC_AUTH_TOKEN = ;
  ** Retail Demo tenant **;
  %Gen_JWT(tenant_id         = %str(&tenant_id.),
           secret_key        = %str(&secret.),
           out_macrovar_name = DSC_AUTH_TOKEN) ;

  **************************************************************************************************;  
  ** 1. Execute PROC HTTP call to upload the descriptor file and get signed URL                  ***;
  **************************************************************************************************;

  filename signurl TEMP ;
  filename desc "&DescriptorFile." ;

  %Call_Proc_HTTP(url                     =&EndPoint.,
                  headerList              =%str("Content-Type"     = "application/vnd.sas.marketing.analytics.product.list.descriptor+json"
                                                "Accept"           = "application/vnd.sas.marketing.analytic.transfer+json" 
                                                "tenantExternalId" = "&tenant_id." 
                                                "Authorization"    = "Bearer %superq(DSC_AUTH_TOKEN)"),
                  InFile                  =desc,
                  Method                  =POST,
                  jsonOutFileNm           =signurl);

  libname jsondata json fileref=signurl ; 

  data _transferID_ ;
    set jsondata.root ;
    call symputx("secureURL",secureURL) ;
    call symputx("TransferID",transferId) ;
    ** convert from UNIX datetime to SAS datetime **;
    creationTimeStamp = (creationTimeStamp/1000 + 315619200);
    expiryTimeStamp = (expiryTimeStamp/1000 + 315619200);
    file LOG ;
    format creationTimeStamp expiryTimeStamp datetime27. ;
    put creationTimeStamp= expiryTimeStamp=  ;
  run ;

  libname jsondata CLEAR ;

  **************************************************************************************************;  
  ** 2. Execute PROC HTTP call with IN= and method=PUT to upload the data to CI360              ****;
  **************************************************************************************************;
 
  filename _in_ "&CSVListFile." ;
  filename _json_ TEMP ;

  %Call_Proc_HTTP(url           =%superq(secureUrl),
                 Method         =PUT,
                 InFile         =_in_,                  
                 jsonOutFileNm  =_json_) ;

  **************************************************************************************************;  
  ** 3. Check the status of the upload                                                          ****;
  **************************************************************************************************;

  %let done = 0 ;

  %do %while (NOT &done.) ;

    filename _json_ TEMP ;

    ** A return code of 200 is the only indication of success we will get? **;
    %Call_Proc_HTTP(url           =&EndPoint./&TransferID.,
                    headerList    =%str("Content-Type"     = "application/vnd.sas.marketing.analytics.product.list.descriptor+json"
                                        "Accept"           = "application/vnd.sas.marketing.analytic.transfer+json" 
                                        "tenantExternalId" = "&tenant_id." 
                                        "Authorization"    = "Bearer %superq(DSC_AUTH_TOKEN)"),
                    Method         =GET,                 
                    jsonOutFileNm  =_json_) ;

    libname jsondata json fileref=_json_ ;

    data ImportStatus ;
      set jsondata.root ;
      file log ;
      put "Import Status: " state ;
      if upcase(state) NE "PENDING" then 
        call symputx("done","1") ;
      x = sleep(10,1) ; 
      drop x ;
    run ;

  %end ;

%mend ;
