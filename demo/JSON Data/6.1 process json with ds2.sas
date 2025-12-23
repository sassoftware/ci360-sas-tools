/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

/* From: https://communities.sas.com/t5/SAS-Communities-Library/Using-DS2-to-parse-JSON-from-a-REST-API/ta-p/229885 */

/* DS2 program that uses a REST-based API */
/* Uses http package for API calls       */
/* and the JSON package (new in 9.4m3)   */
/* to parse the result.                  */
/* Using DS2 requires quite a bit of fore knowledge about the structure 
   and fields within the JSON payload. It also requires many lines of code to 
   extract each field that I want. JSON libname often is a simpler approach */

proc ds2; 
  data messages (overwrite=yes);
    /* Global package references */
    dcl package json j();

    /* Keeping these variables for output */
    dcl double post_date having format datetime20.;
    dcl int views;
    dcl nvarchar(128) subject author board;
    
    /* these are temp variables */
    dcl varchar(65534) character set utf8 response;
    dcl int rc;
    drop response rc;

    method parseMessages();
      dcl int tokenType parseFlags;
      dcl nvarchar(128) token;
      rc=0;
      * iterate over all message entries;
      do while (rc=0);
        j.getNextToken( rc, token, tokenType, parseFlags);

        * subject line;
        if (token eq 'subject') then
          do;
            j.getNextToken( rc, token, tokenType, parseFlags);
            subject=token;
          end;

        * board URL, nested in an href label;
        if (token eq 'board') then
          do;
            do while (token ne 'href');
               j.getNextToken( rc, token, tokenType, parseFlags );
            end;
            j.getNextToken( rc, token, tokenType, parseFlags );
            board=token;
          end;

        * number of views (int), nested in a count label ;
        if (token eq 'views') then
          do;
            do while (token ne 'count');
               j.getNextToken( rc, token, tokenType, parseFlags );
            end;
            j.getNextToken( rc, token, tokenType, parseFlags );
            views=inputn(token,'5.');
          end;

        * date-time of message (input/convert to SAS date) ;
        * format from API: 2015-09-28T10:16:01+00:00 ;
        if (token eq 'post_time') then
          do;
            j.getNextToken( rc, token, tokenType, parseFlags );
            post_date=inputn(token,'anydtdtm26.');
          end;

        * user name of author, nested in a login label;
        if (token eq 'author') then
          do; 
            do while (token ne 'login');
               j.getNextToken( rc, token, tokenType, parseFlags );
            end;
            * get the author login (username) value;
            j.getNextToken( rc, token, tokenType, parseFlags );
            author=token;
            output;
          end;
      end;
      return;
    end;

    method init();
      dcl package http webQuery();
      dcl int rc tokenType parseFlags;
      dcl nvarchar(128) token;
      dcl integer i rc;

      /* create a GET call to the API                                         */
      /* 'sas_programming' covers all SAS programming topics from communities */
      webQuery.createGetMethod(
         'http://communities.sas.com/kntur85557/' || 
         'restapi/vc/categories/id/sas_programming/posts/recent' ||
         '?restapi.response_format=json' ||
         '&restapi.response_style=-types,-null&page_size=100');
      /* execute the GET */
      webQuery.executeMethod();
      /* retrieve the response body as a string */
      webQuery.getResponseBodyAsString(response, rc);
      rc = j.createParser( response );
      do while (rc = 0);
        j.getNextToken( rc, token, tokenType, parseFlags);
        if (token = 'message') then
          parseMessages();
      end;
    end;

  method term();
    rc = j.destroyParser();
  end;

  enddata;
run;
quit;

/* Add some basic reporting */
proc freq data=messages noprint;
    format post_date datetime11.;
    table post_date / out=message_times;
run;

ods graphics / width=2000 height=600;
title '100 recent message contributions in SAS Programming';
title2 'Time in GMT';
proc sgplot data=message_times;
    series x=post_date y=count;
    xaxis minor label='Time created';
    yaxis label='Messages' grid;
run;

title 'Board frequency for recent 100 messages';
proc freq data=messages order=freq;
    table board;
run;

title 'Detailed listing of messages';
proc print data=messages;
run;

title;