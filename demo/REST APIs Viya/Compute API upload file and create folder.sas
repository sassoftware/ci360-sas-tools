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

******************************************************;
**  What are the contexts available in Viya Compute **;
******************************************************;

filename resp temp ;

proc http 
  url = "&hostname./compute/contexts"    
  method=GET
  out= resp ;
  headers
    "Accept"        = "application/json, application/vnd.sas.collection+json" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname contexts json fileref=resp ; 

****************************************************************************;
**  Create a new session in the specified context - copy from prior data  **;
****************************************************************************;

** note contextID that we want to use - standard SAS Studio context **;
%let contextID = 13028236-7c4e-4ef9-947d-8e490bea4345 ;

filename resp temp ;

proc http 
  url = "&hostname./compute/contexts/&contextID./sessions"    
  method=POST
  out=resp  ;
  headers
    "Accept"        = "application/vnd.sas.compute.session+json, application/json, application/vnd.sas.error+json" 
    "Content-Type"  = "application/json"
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname sess json fileref=resp ; 

data _null_ ;
  set sess.root ;
  call symput("sessionID",strip(id)) ;
run ;

**********************************************************************************;
**  Create a new file reference to the folder where we want to upload/download  **;
**********************************************************************************;

filename resp temp ;

proc http 
  url = "&hostname./compute/sessions/&sessionID./filerefs"    
  method=POST
  in='{"name":"casuser","path":"/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/casuser/"}'
  out=resp  ;
  headers
    "Accept"        = "application/json, application/vnd.sas.compute.fileref+json, application/vnd.sas.error+json" 
    "Content-Type"  = "application/json"
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname fref json fileref=resp ; 

**********************************************************************************;
**      list files available in the directory fileref we created earlier        **;
**********************************************************************************;

filename resp temp ;

proc http 
  url = "&hostname./compute/sessions/&sessionID./filerefs/casuser/content?limit=50"    
  method=GET
  out=resp ;
  headers
    "Accept"        = "application/json, application/vnd.sas.collection+json, application/vnd.sas.error+json" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname files json fileref=resp ; 

**********************************************************************************;
**     Create a new file reference to a specific file we want to download       **;
**********************************************************************************;

filename resp temp ;

proc http 
  url = "&hostname./compute/sessions/&sessionID./filerefs"    
  method=POST
  in='{"name":"vaers","path":"/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/casuser/2016VAERSSYMPTOMS.csv"}'
  out=resp  ;
  headers
    "Accept"        = "application/json, application/vnd.sas.compute.fileref+json, application/vnd.sas.error+json" 
    "Content-Type"  = "application/json"
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname fref2 json fileref=resp ; 

**********************************************************************************;
**      Download the file associated with the fileref we just created           **;
**      Note the documentation does NOT have the correct accept value           **;
**********************************************************************************;

filename savefile "C:\temp\2016VAERSSYMPTOMS.csv";

proc http 
  url = "&hostname./compute/sessions/&sessionID./filerefs/vaers/content"     
  method=GET
  out=savefile ;
  headers
    "Accept"        = "application/octet-stream" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

**************************************************************************************;
**  get directory members - must change each slash including leading slash to ~fs~  **;
**************************************************************************************;

filename resp temp ;

**
/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com
~fs~innovationlab-export~fs~innovationlab~fs~homes~fs~Noah.Powers@sas.com
**;

proc http 
  url = "&hostname./compute/sessions/&sessionID./files/~fs~innovationlab-export~fs~innovationlab~fs~homes~fs~Noah.Powers@sas.com/members"    
  method=GET
  out=resp  ;
  headers
    "Accept"        = "application/json, application/vnd.sas.collection+json, application/vnd.sas.error+json" 
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname mems json fileref=resp ; 

**************************************************************************************;
**                     Upload file works the first time                             **;
** see next step for how to get the e-tag for the file so can overwrite later       **;
**************************************************************************************;

filename loadfile "C:\temp\hmeq.csv";
filename resp temp ;

%let filepath = ~fs~innovationlab-export~fs~innovationlab~fs~homes~fs~Noah.Powers@sas.com~fs~casuser~fs~hmeq.csv ;

proc http 
  url = "&hostname./compute/sessions/&sessionID./files/&filepath./content"    
  method=PUT
  in=loadfile
  out=resp  ;
  headers
    "Accept"        = "application/json, application/vnd.sas.compute.file.properties+json, application/vnd.sas.error+json" 
    "Content-Type"  = "application/octet-stream"
/*     "If-Match"      = '"k6p1b0djag"' */
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname upload json fileref=resp ; 

**************************************************************************************;
**               Get E-tag for a file  (only in output header and log)              **;
**************************************************************************************;

filename resp temp ;
filename head "C:\temp\header.txt";

%let filepath = ~fs~innovationlab-export~fs~innovationlab~fs~homes~fs~Noah.Powers@sas.com~fs~casuser~fs~hmeq.csv ;

proc http 
  url = "&hostname./compute/sessions/&sessionID./files/&filepath."    
  method=GET
  headerout=head 
  out=resp  ;
  headers
    "Accept"        = "application/json, application/vnd.sas.compute.file.properties+json, application/vnd.sas.error+json"     
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname finfo json fileref=resp ; 

data _null_ ;
  infile head ;
  input ;
  if length(strip(_infile_)) >= 5 ;
  if upcase(substr(_infile_,1,4)) = "ETAG" then call symput("Etag",compress(substr(_infile_,7),'" ')) ;
run ;
  
%put E-tag=&Etag. ;

**************************************************************************************;
**                          create a new folder                                     **;
**************************************************************************************;

filename resp temp ;
%let filepath = ~fs~innovationlab-export~fs~innovationlab~fs~homes~fs~Noah.Powers@sas.com~fs~casuser  ;

proc http 
  url = "&hostname./compute/sessions/&sessionID./files/&filepath."    
  method=POST
  in='{"name" : "newFolder", "isDirectory": true, "readOnly" : false}'
  out=resp  ;
  headers
    "Accept"        = "application/json, application/vnd.sas.compute.file.properties+json, application/vnd.sas.error+json" 
    "Content-Type"  = "application/json"
    "Authorization" = "Bearer &bearer_token." ;
  debug level= 1 ;
run;

libname newfold json fileref=resp ; 




