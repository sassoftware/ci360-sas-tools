/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Gen_JWT.sas
/ Version   : 2.0
/ Author    : Noah Powers
/ Created   : 2019 August
/ LastModBy : Noah Powers
/ LastModDt : 3.24.2021
/ Purpose   : Generate the JSON Web Token for CI360 that is based on the tenant ID and 
/             secret key.  A Python script to generate the JWT is embedded in this macro
/           
/ FuncOutput: N/A
/ Usage     :
/ Notes     : This macro now uses proc fcmp to invoke python in order to reomove the 
/             dependency on X commands (XCMD) being allowed by SAS. But invoking python 
/             functions with proc fcmp requires the following 
/             environment variables to be defined:
/
/             MAS_M2PATH D:\opt\sasinside\SASHome\SASFoundation\9.4\tkmas\sasmisc\mas2py.py
/             MAS_PYPATH D:\Software\Python3\python.exe
/ 
/             The value of MAS_M2PATH is the path where the mas2py.py file is found in local file system
/             The value of MAS_PYPATH is the python executable in local file system
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/ tenant_id         Tenant ID      
/ secret_key        Secret Key      
/ method            (python/datastep) The default method uses proc fcmp to invoke python 
/                    script to generate the JWT - this requires python to be installed and 
/                    some environment variables be set appropriately.  The datastep method
/                    just uses SAS data step functions. Not sure what minimum version of
/                    SAS is needed for these functions to exist.
/ out_macrovar_name Default=AUTH_TOKEN.  This is the name of the macro variable that will
/                   hold the value of the JWT
/============================================================================================*/
%macro Gen_JWT(tenant_id         =,
               secret_key        =,
               method            =datastep,
               out_macrovar_name =AUTH_TOKEN
               ) ;

  %local _token_ ;
  
  %if ("%upcase(&method.)" NE "PYTHON") AND ("%upcase(&method.)" NE "DATASTEP") %then %do ;
    %put Error: Invalid value for METHOD (&method.) this must be python or datastep ;
    %goto FINISH ;
  %end ;

  %if ("%upcase(&method.)" = "DATASTEP") %then %do ;
    data _null_;
      length digest token $10000. ;
      header    = '{"alg":"HS256","typ":"JWT"}';
      payload   = '{"clientID":"' || strip(symget("TENANT_ID")) || '"}';
      encHeader = translate(put(strip(header),$base64x64.), "-_ ", "+/=");
      encPayload= translate(put(strip(payload),$base64x64.), "-_ ", "+/=");
      key       = put(strip(symget("SECRET_KEY")),$base64x100.);
      digest    = sha256hmachex(strip(key),catx(".",encHeader,encPayload), 0);
      encDigest = translate(put(input(digest,$hex64.),$base64x100.), "-_ ", "+/=");
      token     = catx(".", encHeader,encPayload,encDigest);
      call symputx("_TOKEN_",token);
    run;
  %end ;
  %else %do ;
    filename _py_jwt_ TEMP ;
  
    data _null_ ;
      file _py_jwt_ lrecl=300 ;
      put "def Generate_jwt(tenantId, secretKey):" ;
      put '  "Output: jwtoken"' ;
      put "  import sys, getopt" ;
      put "  import http.client" ;
      put "  import urllib" ;
      put "  import re" ;
      put "  import base64" ;
      put "  import jwt" ;
      put " " ;
      put "  secretKey = bytes(secretKey,encoding='ascii')" ;
      put " ";
      put "  #encode the encoded secret" ;
      put "  encodedSecret = base64.b64encode(secretKey)" ;
      put "  #Generate the JWT" ;
      put "  token = jwt.encode({'clientID': tenantId}, encodedSecret, algorithm='HS256')" ;
      put "  jwt = bytes.decode(token)" ;
      put "  return jwt" ; 
    run ;
   
    proc fcmp ;
      length sas_out $ 200 ;
      /* Declare Python object */
      declare object py(python);
       
      /* Use the INFILE method to import Python code from a file */
      rc = py.infile("%sysfunc(pathname(_py_jwt_))");
       
      /* Publish the code to the Python interpreter */
      rc = py.publish();
       
      /* Call the Python function from SAS */
      rc = py.call("Generate_jwt", "&tenant_id.", "&secret_key.");
       
      /* Store the result in a SAS variable and examine the value */
      sas_out =  py.results["jwtoken"] ;
      put sas_out= ; 
      call symput("_token_",sas_out) ;
    quit ;
  %end ;
  
  %let &out_macrovar_name.=&_token_. ;
  %put &out_macrovar_name.=&&&out_macrovar_name. ;

  %FINISH:
%mend ;
                