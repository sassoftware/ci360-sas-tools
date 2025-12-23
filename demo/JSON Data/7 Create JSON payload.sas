/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

filename payload1 "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/export/payload1.txt" ;
filename payload2 "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/export/payload2.txt" ;

%let contentName = mycontent ;
%let dataDescriptorId = X12345 ;
%let fieldDelimiter = | ;
%let fileLocation = C:\mydata\mydata.dlm ;
%let filetype = delimited ;
%let headerRowIncluded = Y ;
%let updateMode = N ;

/* use put statements to create the JSON text file */
data _null_ ;
  file payload1 LRECL=20000 ;
  put "{" ;
  put '"contentName": "' "&contentName." '",' ;
  put '"dataDescriptorId": "' "&DataDescriptorID." '",' ;
  put '"fieldDelimiter": "' "&fieldDelimiter." '",' ;
  put '"fileLocation": "' "&fileLocation." '",' ;
  put '"fileType": "' "&fileType." '",' ;
  put '"headerRowIncluded": ' "&headerRowIncluded., " ;
  put '"recordLimit": 0,' ;
  put '"updateMode": "' "&updateMode." '"' ;
  put "}" ;
run ;

/* Use proc JSON to create the text file */
proc json pretty out=payload2 nosastags ;
  write open object ; /* open outermost object */
    write values "contentName" "&contentName." ;
    write values "dataDescriptorId" "&DataDescriptorID." ;
    write values "fieldDelimiter" "&fieldDelimiter." ;
    write values "fileLocation" "&fileLocation." ;
    write values "fileType" "&fileType." ;
    write values "headerRowIncluded" "&headerRowIncluded." ;
    write values "recordLimit" 0 ;
    write values "updateMode" "&updateMode." ;
  write close ;
run ;
  
  
  