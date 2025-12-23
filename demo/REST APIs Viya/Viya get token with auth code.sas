/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

/* 
 Details in this code were sourced from this blog post:
 https://blogs.sas.com/content/sgf/2023/02/07/authentication-to-sas-viya/

 Authorization code grant type does require user to login... 

 Info you need for this grant type:
 1. Hostname of your Viya environment 
 2. client ID & Client secret (from your Viya Admin).  Don't share either of these values
 
 First, copy this URL into browser and copy the code value returned in the browser into the code macro parameter value below
 <my hostname>/SASLogon/oauth/authorize?client_id=<myclientid>&response_type=code 
 https://innovationlab.demo.sas.com/SASLogon/oauth/authorize?client_id=<myclientID>&response_type=code 
 
 Then execute the proc http equivalent of the following cURL (see below):
 
 curl -k https://sasserver.demo.sas.com/SASLogon/oauth/token \
     -H "Accept: application/json" -H "Content-Type: application/x-www-form-urlencoded" \
     -u "myclientid:myclientsecret" -d "grant_type=authorization_code&code=YZuKQUg10Z"
 
  As long as the refresh token is not expired, can use this later in one step process to authenticate 
  that skips the pasting the URL into the browser to get the auth code.
  
  curl -k https://sasserver.demo.sas.com/SASLogon/oauth/token -H "Accept: application/json" \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -u "myclientid:myclientsecret" \
     -d "grant_type=refresh_token&refresh_token=$REFRESH_TOKEN"
  
*/

** Paste code from browser and run to get bearer token **;
%let hostname = https://innovationlab.demo.sas.com;
%let code = ;

filename pw "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/Viya_client_id_secret.txt" ;

data _null_ ;
  infile pw dsd MISSOVER LRECL=60000;
  length key $2000 ;
  input key $ ;
  *put key= ;
  if _N_ = 1 then call symput("client_id",trim(left(key))) ;
  else if _N_ = 2 then call symput("client_secret",trim(left(key))) ;
run ;

%*put client_id=&client_id. ;
%*put client_secret=&client_secret. ;

** Save the response **;
filename response "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/token.json"  ;

** Get both access token and refresh token **;
proc http 
  url = "&hostname./SASLogon/oauth/token"    
  method=POST
  webusername="&client_id."
  webpassword="&client_secret."
  in="grant_type=authorization_code%nrstr(&)code=&code."
  out= response ;
  headers
    "Accept"        = "application/json"
    "Content-Type"  = "application/x-www-form-urlencoded" ;
  debug level= 1 ;
run;

libname jsondata json fileref=response ; 

data _null_ ;
  set jsondata.root ;
  call symput("bearer_token",strip(access_token)) ;
  call symput("refresh_token",strip(refresh_token)) ;
run ;

%put &bearer_token ;

** 2 When access token expires, can use refresh token to authenticate and get new access token **;
filename resp temp ;

proc http 
  url = "&hostname./SASLogon/oauth/token"    
  method=POST
  webusername="&client_id."
  webpassword="&client_secret."
  in="grant_type=refresh_token%nrstr(&)refresh_token=&refresh_token."
  out= resp ;
  headers
    "Accept"        = "application/json"
    "Content-Type"  = "application/x-www-form-urlencoded" ;
  debug level= 1 ;
run;

** Note the access token is new, and the refresh token remains static.  **;
libname jsondata json fileref=resp ; 

data _null_ ;
  set jsondata.root ;
  call symput("bearer_token",strip(access_token)) ;
run ;