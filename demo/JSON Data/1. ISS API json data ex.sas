/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

/* https://blogs.sas.com/content/sasdummy/2016/12/02/json-libname-engine-sas/

Simple JSON example: Who is in space right now?
Speaking of skyrocketing, I discovered a cool web service that reports who is in space right now 
(at least on the International Space Station). It's actually a perfect example of a REST API, 
because it does just that one thing and it's easily integrated into any process, including SAS. 
It returns a simple stream of data that can be easily mapped into a tabular structure. 
*/

** Use a temporary file if you dont want to see the json text **;
filename resp temp ; 
filename resp "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/export/iss_people.json";
 
/* Neat service from Open Notify project */
proc http 
 url="http://api.open-notify.org/astros.json"
 method= "GET"
 out=resp;
run;
 
/* Assign a JSON library to the HTTP response */
libname space JSON fileref=resp;
 
/* Print result, dropping automatic ordinal metadata */
title "Who is in space right now? (as of &sysdate)";
proc print data=space.people (drop=ordinal:);
run;