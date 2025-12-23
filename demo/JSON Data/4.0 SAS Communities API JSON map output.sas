/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

/* https://blogs.sas.com/content/sasdummy/2016/12/02/json-libname-engine-sas/

The following code leverages the SAS Support Communities API to extract
message information from the site into JSON output
*/

/* split URL for readability */
%let url1=http://communities.sas.com/kntur85557/restapi/vc/categories/id/analytics/topics/recent;
%let url2=?restapi.response_format=json%str(&)restapi.response_style=-types,-null,view;
%let url3=%str(&)page_size=2;
%let fullurl=&url1.&url2.&url3;

** Use a temporary file if you dont want to see the json text **; 
filename topics temp;
filename topics "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/json data/topics.json";

** Create a placeholder file for the JSON mapping file *; 
** Which will capture the structure of the JSON data in another JSON file **;
filename jmap "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/json data/jsonmap.txt";
 
proc http
 url= "&fullurl."
 method="GET"
 out=topics;
run;
 
libname posts JSON fileref=topics map=jmap automap=create;