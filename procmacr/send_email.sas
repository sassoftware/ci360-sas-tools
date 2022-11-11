/*
/===========================================================================================
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/
/ Program   : Send_email.sas
/ Author    : Noah Powers
/ Created   : 2022
/ Purpose   : To send an email possibly with a table printed in the body 
/ FuncOutput: N/A
/ Usage     :
/ Notes     : 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ fromAddress    The email address to display as the sender of the email
/ toAddress      The email address to send the email to or a space delimited list of addresses
/ subject        The subject of the email
/ emailHost      (optional) If specified this is used to set the EMAILHOST SAS option
/ emailPort      (optional) If specified this is used to set the EMAILPORT SAS option
/ title          (optional) If provided and there is a table specified, then this is
/                the title printed above the title
/ table_in_body  (optional) If provided, the table is printed in the body of the email
/ max_rows2print default = 100.  The max number of rows of the table to print in the email
/============================================================================================*/
%macro Send_Email(fromAddress          =,
                  toAddress            =,
                  subject              =,
                  emailhost            =,
                  emailport            =,                  
                  title                =,
                  table_in_body        =,
                  max_rows2print       =100
                  ) ;       

  %if %length(&emailhost.) %then %do ;
    options emailhost &emailhost. ;
  %end ;
  %if %length(&emailport.) %then %do ;
    options emailport &emailport. ;
  %end ;
                                  
  filename _mail_ email from="&fromAddress."
                        to=(%quotelst(&toAddress.))
                        ct="text/html"
                        subject="&subject." ;
 
  ods html body=_mail_ style=htmlblue rs=none ;
  
  %if %length(&table_in_body.) %then %do ;

    title "&title." ;
    proc print data=&table_in_body. (obs=&max_rows2print.); 
    run;

  %end ;

  ods html close;
  filename _mail_ clear;
 
%mend ; 
