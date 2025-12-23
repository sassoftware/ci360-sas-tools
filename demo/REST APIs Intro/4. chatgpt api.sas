/*
  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

options linesize=180 LRECL=32000 ;

/* Define the API endpoint and your API key */
%let api_endpoint = https://api.openai.com/v1/chat/completions ;
%let api_Organization = org-SOfIeUPUy7JIjbCODCaKcvN3 ;
%let api_Project = proj_CeY0GzypmX6vnuMh3ARjePDl;

%let api_key = ;

filename apikey "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/chatGPT_API_key.txt" ;
data _null_ ;
  infile apikey dsd MISSOVER LRECL=60000;
  length key $2000 ;
  input key $ ;
  *put key= ;
  call symput("api_key",trim(left(key))) ;
run ;

filename payload temp ;

data _null_ ;
  file payload;
  put "{" ;
  put '"model": "gpt-4",' ;
  put '"messages": [' ;
  put '  {"role": "developer", "content": "You are a helpful SAS coding assistant."},' ;
  put '  {"role": "user", "content": "Provide me with sample SAS code to get pearson correlations using SASHELP data"}' ;
  put '  ]' ;
  put "}" ;
run ;

filename payload2 "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/tmp/chatgtp_payload.json" ;

proc json pretty out=payload2 nosastags ;
  write open object ; /* open outermost object */
    write values "model" "gpt-4" ;
    write values "messages" ;
    write open array ;
      write open object ;
        write values "role" "developer" ;
        write values "content" "You are a helpful SAS coding assistant." ;
      write close ;
      write open object ;
        write values "role" "user" ;
        write values "content" "Provide me with sample SAS code to get pearson correlations using SASHELP data" ;
      write close ;
    write close ;
  write close ;
run ;

/* Make the HTTP POST request 
CLEAR_CACHE: specifies to clear both the shared connection and cookie caches before the HTTP request is executed.
HEADEROUT_OVERWRITE: used in conjunction with the HEADEROUT= argument, causes the response header to record only 
                     the last header block sent by the web server when a redirect occurs.
*/

filename hdrout "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/tmp/chatgtp_hdrOut.txt" ;
filename response "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/tmp/chatgtp_response.json" ;

proc http HEADEROUT_OVERWRITE CLEAR_CACHE
    url="&api_endpoint"
    method="POST"
    headerout=hdrout
    out=response 
    in=payload ;
    headers "Authorization" = "Bearer &api_key"
            "Content-Type" = "application/json" 
            "OpenAI-Organization" = "&api_Organization."
            "OpenAI-Project" = "&api_Project." ;
    debug level=1 ;
run;

libname jsonresp json fileref=response ; 

proc print data=jsonresp.choices_message ;
 var content ;
run ;
