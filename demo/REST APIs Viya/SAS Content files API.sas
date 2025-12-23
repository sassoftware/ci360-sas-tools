/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

/*
curl --request GET \
  --url http://example.com/files/ \
  --header 'Accept: application/vnd.sas.api+json, application/json' \
  --header 'Authorization: Bearer <access-token-goes-here>'
 */

filename token "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/token.json"  ;

libname jsondata json fileref=token ; 

data _null_ ;
  set jsondata.root ;
  call symput("bearer_token",strip(access_token)) ;
run ;

%*put &bearer_token ;
%let hostname = https://innovationlab.demo.sas.com;

*********************************************************;
**  These are all available links including shortcuts  **;
*********************************************************;

filename resplist temp ;

proc http 
  url = "&hostname./files"    
  method=GET
  out= resplist ;
  headers
    "Accept"        = "application/json, application/vnd.sas.api+json" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname flinks json fileref=resplist ; 

*********************************************************;
**    These are all available files created by me      **;
*********************************************************;

filename resp temp ;

proc http 
  url = "&hostname./files/files?filter=eq(createdBy,'Noah.Powers@sas.com')%nrstr(&)limit=50"    
  method=GET
  out= resp ;
  headers
    "Accept"        = "application/json, application/vnd.sas.collection+json, application/vnd.sas.error+json" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname myfiles json fileref=resp ; 

*****************************************************************************;
**    Example how to handle when many outputs using start= and limit=      **;
**  Here we get dataset with all of my .sas files                          **;
*****************************************************************************;

%macro doit() ;
 
  data _null_ ;
    set myfiles.root ;
    call symput("Numfiles",strip(put(count,8.))) ;
  run ;
  
  proc datasets library=work nolist ;
    delete allfiles ;
  quit ;
  
  %let start = 0 ;
  %let limit = 50 ;
  %do %while (%eval(&start. <= &Numfiles. - 1)) ;
  
    %let endpoint = /files/files?filter=eq(createdBy,'Noah.Powers@sas.com')%nrstr(&)sortBy=name%nrstr(&)start=&start.%nrstr(&)limit=&limit. ;
    filename resp temp ;

    **  These are all available files created by me **;
    proc http 
      url = "&hostname.&endpoint."    
      method=GET
      out= resp ;
      headers
        "Accept"        = "application/json, application/vnd.sas.collection+json, application/vnd.sas.error+json" 
        "Authorization" = "Bearer &bearer_token." ;
      debug level= 1 ;
    run;
    
    libname myfiles json fileref=resp ; 
    
    data sasfiles ;
      set myfiles.items ;
      where scan(lowcase(name),-1,".") = "sas" ;
    run ;
    
    proc append base=allfiles data=sasfiles FORCE ;
    run ;
    
    %let start = %eval(&start. + &limit.) ;
    
  %end ;
  
%mend ;

%doit() ;

*********************************************************;
**    Download file from SAS Content folders           **;
*********************************************************;

** Copy fileID from the WORK.allfiles for the autoexec.sas file **;
%let fileID = 0cbb3a4b-9fbc-4459-8cec-a7a5f220aa4d ;

** Choose filename and location to save file in filename **;
filename resp2 "C:\temp\autoexec.txt" ;

** download single file using file ID from previous http call **;
proc http 
  url = "&hostname./files/files/&fileID./content"    
  method=GET
  out= resp2 ;
  headers
    "Accept"        = "*/*" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;



