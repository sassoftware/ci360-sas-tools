/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.
*/

/*
Example: Using SQL and proc transpose to combine JSON tables
*/

proc transpose data=wiki.records_phonenumbers out=phoneNum_T (drop=_name_) ;
  by ordinal_records ;
  id type  ;
  var number ;
run ;

proc sql;
 create table Combined_sql (drop=ordinal_records ordinal_address) as select 
   rec.LastName, rec.firstName, rec.age, rec.isAlive, rec.spouse, phoneNum.*, add.*,
   child.children1 as child1, child.children2 as child2, child.children3 as child3
   
   from wiki.records as rec left join phoneNum_T as phoneNum 
     on (rec.ordinal_records = phoneNum.ordinal_records)
     
     left join wiki.records_address as add 
     on (phoneNum.ordinal_records = add.ordinal_records)
     
     left join wiki.records_children as child 
     on (add.ordinal_records = child.ordinal_records)
     ;
quit;

/*
Example: More advanced using SQL and macro vars to avoid hard coding child
*/

proc contents data=wiki.records_children noprint out=vars (keep=name) ;
run ;

proc sql noprint ;
  select count(*) into: numChildren from vars (where=(upcase(substr(name,1,8)) = "CHILDREN")) ;
quit ;

%macro renameVars() ;
  %let renamelist = ;
  %do i = 1 %to &numChildren. ;
    %let renameList = &renamelist. child.children&i. as child&i. ;
    %if &i. < &numChildren. %then %let renameList = &renameList., ;
  %end ;

  &renameList. 
%mend ;

proc sql;
 create table Combined_sql2 (drop=ordinal_records ordinal_address) as select 
   rec.LastName, rec.firstName, rec.age, rec.isAlive, rec.spouse, phoneNum.*, add.*,
   %renameVars() 
   
   from wiki.records as rec left join phoneNum_T as phoneNum 
     on (rec.ordinal_records = phoneNum.ordinal_records)
     
     left join wiki.records_address as add 
     on (phoneNum.ordinal_records = add.ordinal_records)
     
     left join wiki.records_children as child 
     on (add.ordinal_records = child.ordinal_records)
     ;
quit;

/*
Example: Using DATA steps to combine JSON tables
*/

data children ;
  merge wiki.records 
        wiki.records_address
        wiki.records_children
  ;
  by ordinal_records;
  if not (first.ordinal_records and last.ordinal_records) then abort ;
  drop ordinal_children ordinal_address ;
run;

data Combined_ds ;
  merge children 
        wiki.records_phoneNumbers (drop=ordinal_phoneNumbers)
  ;
  by ordinal_records ;
  retain home office mobile ;  ** this was originally missing - causing incorrect results **;
  array phone (*)  $12. home office mobile ;
  if first.ordinal_records then do i = 1 to dim(phone) ;
    phone(i) = "" ;
  end ;
  
  if upcase(type) = "HOME" then home = strip(number) ;
  else if upcase(type) = "OFFICE" then office = strip(number) ;
  else if upcase(type) = "MOBILE" then mobile = strip(number) ;
  
  if last.ordinal_records then output ;
  
  drop i number type ordinal_root ;
run ;
  