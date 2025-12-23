/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

filename token "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/token.json"  ;

libname jsondata json fileref=token ; 

data _null_ ;
  set jsondata.root ;
  call symput("bearer_token",strip(access_token)) ;
run ;

%*put &bearer_token ;
%let hostname = https://innovationlab.demo.sas.com;

***************************************;
** Get casmanagement resource links  **;
***************************************;

filename resp temp ;

proc http 
  url = "&hostname./casManagement/"    
  method=GET
  out= resp ;
  headers
    "Accept"        = "application/json, application/vnd.sas.api+json" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname casmgmt json fileref=resp ; 

***************************************;
** what CAS servers are available?   **;
***************************************;

filename resp temp ;

proc http 
  url = "&hostname./casManagement/servers"    
  method=GET
  out= resp ;
  headers
    "Accept"        = "application/json, application/vnd.sas.collection+json" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname srvs json fileref=resp ; 

*******************************************************************;
** what is the state of the CAS server ? ONly info is in the log **;
*******************************************************************;

%let CASserver = cas-shared-default ;

filename resp temp ;

proc http 
  url = "&hostname./casManagement/servers/&CASserver./state"    
  method=GET
  out= resp ;
  headers
    "Accept"        = "text/plain, application/json" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 3 ;
run;

*******************************************************************;
**                   create a new CAS session                    **;
*******************************************************************;

filename resp temp ;

proc http 
  url = "&hostname./casManagement/servers/&CASserver./sessions"    
  method=POST
  in='{"authenticationType" : "OAuth",
  "id": "mycas",
  "owner": "Noah.Powers@sas.com",
  "state": "Connected",
  "timeOut": 18000}'
  out= resp ;
  headers
    "Accept"        = "application/vnd.sas.cas.session+json, application/json" 
    "Content-Type"  = "application/vnd.sas.cas.session+json"
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname newsess json fileref=resp ; 

data _null_ ;
  set newsess.root ;
  call symput("sessionID",strip(ID)) ;
run ;

*******************************************************************;
**                  get state of the session                     **;
*******************************************************************;

filename resp temp ;

proc http 
  url = "&hostname./casManagement/servers/&CASserver./sessions/&sessionID./state"    
  method=GET
  out= resp ;
  headers
    "Accept"        = "application/json, text/plain" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 3 ;
run;

*******************************************************************;
**         what caslibs are available (in my session)            **;
*******************************************************************;

filename resp temp ;

proc http 
  url = "&hostname./casManagement/servers/&CASserver./caslibs?sessionId=&sessionID.%nrstr(&)limit=50"     
  method=GET
  out= resp ;
  headers
    "Accept"        = "application/json, application/vnd.sas.collection+json" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname clibs json fileref=resp ; 

*******************************************************************;
**       what files are available in a particular caslib         **;
*******************************************************************;

filename resp temp ;

proc http 
  url = "&hostname./casManagement/servers/&CASserver./caslibs/casuser/sources?limit=50"     
  method=GET
  out= resp ;
  headers
    "Accept"        = "application/json, application/vnd.sas.collection+json" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname casfile json fileref=resp ; 

***********************************************************************************************;
**           what tables available in a particular caslib (even if NOT loaded)               **;
***********************************************************************************************;

filename resp temp ;

proc http 
  url = "&hostname./casManagement/servers/&CASserver./caslibs/casuser/tables?limit=50"     
  method=GET
  out= resp ;
  headers
    "Accept"        = "application/json, application/vnd.sas.collection+json" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname castab json fileref=resp ; 

***********************************************************************************************;
**                          upload new file to caslib folder                                 **;
***********************************************************************************************;

filename resp temp ;
filename hmeq "C:\temp\hmeq.csv" ;

proc http 
  url = "&hostname./casManagement/servers/&CASserver./caslibs/casuser/tables"     
  method=POST
  in = MULTI FORM ("tableName"         = "hmeq",
                   "format"            = "csv",
                   "containsHeaderRow" = "True",
                   "file"              = hmeq) 
  out= resp ;
  headers
    "Accept"        = "application/json, application/vnd.sas.collection+json" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname castab json fileref=resp ; 

***********************************************************************************************;
**                          unload a CAS table from memory                                   **;
***********************************************************************************************;

filename resp temp ;

proc http 
  url = "&hostname./casManagement/servers/&CASserver./caslibs/casuser/tables/HMEQ/state?value=unloaded"     
  method=PUT
  out= resp ;
  headers
    "Accept"        = "Accept: application/json, text/plain" 
    "Content-Type"  = "application/json"
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;


