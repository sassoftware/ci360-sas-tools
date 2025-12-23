/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/
/*
From:
https://communities.sas.com/t5/SAS-Communities-Library/How-to-read-JSON-data-in-SAS/tac-p/850405

Reading newline-delimited JSON files (also known as JSONL or NDJSON)
Some systems store and exchange data using newline-delimited JSON. These standards are
 known as JSON Lines (.jsonl) or NDJSON. In a JSONL file, each line of text represents a 
 valid JSON object -- building up to a series of records. But there is no hierarchical 
 relationship among these lines, so when taken as a whole the JSONL file is not valid JSON. 
 That is, a JSON parser can process each line individually, but it cannot process the file 
 all at once. This includes the JSON library engine.

For a simple approach to read this in SAS, convert the JSONL input to a valid JSON by
 adding an object wrapper and comma delimiter between each line. Here's a sample program, 
 adapted from one provided in a solved forum topic:
 https://communities.sas.com/t5/SAS-Programming/Reading-NDJSON-Newline-Delimited-JSON/m-p/665720#M199100
*/

filename json_nl url "https://bcda.cms.gov/assets/downloads/Patient.ndjson";
filename json "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/export/patient.json";
filename map temp;

/* Convert the NDJSON to JSON */
data _null_;
  file json;
  if _n_=1 then put '[' @;
  else if eof then put ']';
  else put ',' @;
  infile json_nl end=eof;
  input;
  put _infile_;
run;

libname json json automap=create map=map;

/* Check resulted Library and data sets */
proc contents data=json._all_;
run;