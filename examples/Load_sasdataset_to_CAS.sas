/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

** Start local CAS session with name **;
cas mazter ;

** Automatically create bound Libnames for exisitng GLOBAL caslibs with sas8 names **;
caslib _all_ assign;

** Create a local named caslib that is linked to the underlying path that contains the SAS data set **;
** There are many other types of inputs that are supported by the this framework: sashdat, csv, etc **;
** Search for Path-Based Data Source Types and Options in the SAS documentation **;
** https://go.documentation.sas.com/?cdcId=pgmsascdc&cdcVersion=v_010&docsetId=casref&docsetTarget=n0kizq68ojk7vzn1fh3c9eg3jl33.htm&locale=en#n0cxk3edba75w8n1arx3n0dxtdrt  **;
caslib myclib datasource=(srctype="path") 
               path="<folder that contains the SAS data set>" ;

** Create a bound LIBNAME to the caslib **;
libname mylib cas caslib=myclib ;

** load the records in the underlying SAS table into the PUBLIC caslib so can work with data in memory **;
proc casutil incaslib="myclib" outcaslib="casuser";    ** specify the CASLIB where the data will be saved **;                
   contents casdata="<SAS dataset name>.sas7bdat";    ** See meta data of underlying SAS table **;
   
   ** lift SAS dataset rows into outcaslib specfied on proc statement **;
   load casdata="<SAS dataset name>.sas7bdat" casout="<name of output in-memory table>";                           
   list tables incaslib="casuser";                                     
   *save casdata="<name of output in-memory table>" incaslib="public";                               
run;

** data manipulation steps in CAS - no need to sort data in memory **;

** save final data to a pre-existing global caslib for other users to see **;
data public.<name> (compress=yes promote=yes)  ;
    set casuser.<name of output in-memory table> ;
    ** statments here **;
run ;

** save data to SAS library on disk **;
data mysaslib.<name> (compress=yes)  ;
    set casuser.<name of output in-memory table> (datalimit=ALL) ;
    ** statments here **;
run ;

** terminate the session when done **;
cas mazter terminate ;