/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

filename wiki "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/export/wiki_json.json";
filename wikimap "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/export/wiki_json_map.json" ;

libname wiki JSON fileref=wiki map=wikimap automap=create ;

/** 1. Download the JSON map file 
    2. Use text edior to modify map file to create fewer tables
    3. Upload the modified map file and use this with the JSON libname
**/

filename wmap_new "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/export/wiki_json_map_new.json" ;
libname wiki2 JSON fileref=wiki map=wmap_new ;

proc transpose data=wiki2.records_phonenumbers out=phoneNum_T (drop=_name_) ;
  by ordinal_records ;
  id type  ;
  var number ;
run ;

data Combined_Map_ds ;
  merge wiki2.records 
        phoneNum_T 
  ;
  by ordinal_records ;
  if not (first.ordinal_records and last.ordinal_records) then abort ;
run ;
