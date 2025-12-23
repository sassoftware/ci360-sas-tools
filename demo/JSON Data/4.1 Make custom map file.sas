/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

filename minmap "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/export/minmap.map";

data _null_;
infile datalines;
file minmap;
input;
put _infile_;
datalines;
{
  "DATASETS": [
    {
      "DSNAME": "messages",
      "TABLEPATH": "/root/response/messages/message",
      "VARIABLES": [
        {
          "NAME": "view_href",
          "TYPE": "CHARACTER",
          "PATH": "/root/response/messages/message/view_href",
          "CURRENT_LENGTH": 136
        },
        {
          "NAME": "id",
          "TYPE": "NUMERIC",
          "PATH": "/root/response/messages/message/id"
        },
        {
          "NAME": "subject",
          "TYPE": "CHARACTER",
          "PATH": "/root/response/messages/message/subject",
          "CURRENT_LENGTH": 84
        },
        {
          "NAME": "view_friendly_date",
          "TYPE": "CHARACTER",
          "PATH": "/root/response/messages/message/post_time/view_friendly_date",
          "CURRENT_LENGTH": 12
        },
        {
          "NAME": "viewDate",
          "TYPE": "NUMERIC",
          "INFORMAT": ["MMDDYY", 10, 0 ],
          "FORMAT": ["MMDDYY", 10],
          "PATH": "/root/response/messages/message/post_time/view_date",
          "CURRENT_LENGTH": 8
        },
        {
          "NAME": "viewTime",
          "TYPE": "NUMERIC",
          "INFORMAT": ["TIME", 8, 0],
          "FORMAT": ["TIME", 8],
          "PATH": "/root/response/messages/message/post_time/view_time",
          "CURRENT_LENGTH": 8
        },
        {
          "NAME": "datetime",
          "TYPE": "NUMERIC",
          "INFORMAT": [ "IS8601DT", 19, 0 ],
          "FORMAT": ["DATETIME", 20],
          "PATH": "/root/response/messages/message/post_time/$",
          "CURRENT_LENGTH": 8
        },
        {
          "NAME": "views",
          "TYPE": "NUMERIC",
          "PATH": "/root/response/messages/message/views/count"
        },
        {
          "NAME": "login",
          "TYPE": "CHARACTER",
          "PATH": "/root/response/messages/message/last_edit_author/login",
          "CURRENT_LENGTH": 15
        },
        {
          "NAME": "likes",
          "TYPE": "NUMERIC",
          "PATH": "/root/response/messages/message/kudos/count"
        }
      ]
    }
  ]
}
;
run;

proc http
 url= "&fullurl."
 method="GET"
 out=topics;
run;

title "Using custom JSON map";
libname posts2 json fileref=topics map=minmap;
proc datasets lib=posts2; quit;

data messages;
 set posts2.messages;
run;

proc print data=messages; run;