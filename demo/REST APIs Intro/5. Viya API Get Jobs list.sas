/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.

Documentation Here:
https://developer.sas.com/rest-apis/jobExecution/getJobs#query-parameters

&filter=and(gt(creationTimeStamp,%272023-07-25T15:51:13.319Z%27)) 
*/

/*Gets URI of your server*/
%let BASE_URI=%sysfunc(getoption(servicesbaseurl));

/*Saves Proc http output to a temp file*/
filename jobs temp;

/*Update Start,Limit,Filter to control what returns*/
/*Oauth_bearer=sas_services uses SAS Studio's authentication to access API*/
proc http
    url="&base_uri./jobExecution/jobs?filter=eq(createdBy,'Noah.Powers@sas.com')%nrstr(&)start=0%nrstr(&)limit=20"
    method='get'
    oauth_bearer=sas_services
    out=jobs;
    debug level= 1 ;
run;

/*Maps output JSON file to SAS table*/
libname jobs json;

/*Example of Common Tables*/
proc print data=jobs.items (obs=10);
run;

