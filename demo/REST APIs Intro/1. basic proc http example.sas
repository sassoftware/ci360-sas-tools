/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

/*
   These examples leverage httpbin.org site which does not require any
   Authentication unlike most APIs.  Very basic good starting point.
*/

/* set up query parameters as macro variables */

%let firstname=Joseph; 
%let lastname=Henry; 
%let company=Stuff & Things Inc.;  

/* Option 1: URL encode the values and re-place encoded values into macro vars */

data _null_;
  length firstname lastname company $200. ;
  firstname = urlencode("&firstname");
  lastname = urlencode("&lastname");
  company = urlencode("&company");
  call symputx("firstname",firstname,'G');
  call symputx("lastname",lastname,'G');
  call symputx("company",company,'G');
run;

filename response temp ;
filename response "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/tmp/httpbin.json" ;

/* Option 1: Pass query string as part of the URL parameter  */

proc http  
  url="https://httpbin.org/get?firstname=&firstname.%nrstr(&)lastname=&lastname.%nrstr(&)company=&company."
  method="GET"
  out=response  ; 
  debug level= 1 ;
run; 

libname jsonresp json fileref=response ; 

libname jsonresp clear ;

/* Option 2: Use the QUERY parameter option -Need SAS 9.4M6+ */

%let firstname=Joseph; 
%let lastname=Henry; 
%let company=Stuff & Things Inc.;  

proc http   
  url="https://httpbin.org/get"   
  method="GET"
  query = ("firstname"="&firstname"            
           "lastname"="&lastname"            
           "company"="&company")
  out=response ;   
  debug level= 1 ;           
run; 

libname jsonresp json fileref=response ; 

