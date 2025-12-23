/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

/* Code from:
https://communities.sas.com/t5/SAS-Communities-Library/How-to-read-JSON-data-in-SAS/tac-p/850405

How the JSON engine maps objects to tables
The JSON libname engine reads a valid JSON file and maps the content to one or more SAS data tables. 
As JSON data often represents a nested series of objects, the JSON engine creates these tables with 
relational keys that you can use to combine the data to fit your needs. The following example illustrates 
how the JSON engine interprets the sample JSON data included in the Wikipedia article about JSON 
(https://en.wikipedia.org/wiki/JSON). First, here's the SAS program to create and read the JSON:
*/

filename wiki "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/export/wiki_json.json";

/* Sample JSON from Wikipedia */
data _null_;
 file wiki;
 infile datalines;
 input;
 put _infile_;
datalines;
[
  {
    "records": [
      {
        "firstName": "John",
        "lastName": "Smith",
        "isAlive": true,
        "age": 27,
        "address": {
          "streetAddress": "21 2nd Street",
          "city": "New York",
          "state": "NY",
          "postalCode": "10021-3100"
        },
        "phoneNumbers": [
          {
            "type": "home",
            "number": "212 555-1234"
          },
          {
            "type": "office",
            "number": "646 555-4567"
          }
        ],
        "children": [
          "Catherine",
          "Thomas",
          "Trevor"
        ],
        "spouse": null
      },
      {
        "firstName": "Jason",
        "lastName": "Brown",
        "isAlive": true,
        "age": 56,
        "address": {
          "streetAddress": "123 Mockingbird Lane",
          "city": "Chicago",
          "state": "IL",
          "postalCode": "60654-3100"
        },
        "phoneNumbers": [
          {
            "type": "home",
            "number": "773 123-1234"
          },
          {
            "type": "mobile",
            "number": "847 555-4567"
          }
        ],
        "children": [
          "Noah",
          "Arianna"
        ],
        "spouse": "Amanda"
      }
    ]
  }
]
;
run;

libname wiki JSON fileref=wiki;

proc datasets lib=wiki nolist nodetails;
 contents data=_all_;
quit;