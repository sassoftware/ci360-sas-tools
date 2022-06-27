/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Get_360Hub_File_MetaData.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2019 November
/ LastModBy : Noah Powers
/ LastModDt : 08.05.2020
/ Purpose   : Create two SAS datasets with the HUB file metadata.  The first output ds is
/             at the file level and the other is at the file-variable level.  These datasets
/             are intended to be used as inputs to subsequent macro calls to download the .gz
/             files and read those gz files into sas datasets.
/ FuncOutput: N/A
/ Usage     :
/ Notes     : - The contact preferences dataset has blank values for variable names but the 
/               labels appear to be valid SAS names.  Added code to fill this in and have told
/               SAS tech support about it.
/             - Users can subset the outTable list dataset to enable users to limit which files
/               they want to process in later steps.
/ Steps:
/
/ 1. get a list of data files available 1 rest call
/ 2. Loop through each data file for variable and table metadata by
/    a. make rest call with data id to get addl table level meta data 
/    b. append this data and cross check with the list from 1.
/    c. get descriptor info: variable level metadata and some addl table metadata combine into single file 
/ Notes:
/  - The endpoints and steps were changed so had to change this code to match
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name            Description
/ -------------------------------------------------------------------------------------
/ JWT             Java Web Token based on the tenant id and secret key.  
/                 The %Gen_JWT() macro can create this for you.
/ url_base        Default is %nrstr(https://extapigwservice-demo.cidemo.sas.com) - change as appropriate for
/                 your tenant
/ limit           Limits the number of links returned at once default is 1000
/ outTableListds  WORK.CI360HubFileList This table contains one row per table with all 
/                 table level metadata returned from API call included
/ outVarListDs    WORK.CI360HubVarList This table contains the column or variable level
/                 metadata and associated tables for each table.
/============================================================================================*/

%macro Get_360Hub_File_MetaData(JWT           =,
                                url_base      =%nrstr(https://extapigwservice-demo.cidemo.sas.com),
                                limit         =1000,
                                outTableListds=WORK.CI360HubFileList,
                                outVarListDs  =WORK.CI360HubVarList) ;

  %local url i curlen maxlen id TableIDList NumHubTables slash droplist ;

  %if (&sysscp. = WIN) %then %do;
    %let slash = %str(\);
  %end;
  %else %do;
    %let slash = %str(/);
  %end ;

  %let url = &url_base./marketingData/tables?limit=&limit. ;

  proc datasets library=work nolist ;
    delete TableAttr / memtype=data ;
  quit ;

   **Get list of data IDs **;
  filename _json_  TEMP ;

  %Call_Proc_HTTP(url                     =%str(&url.),
                  Method                  =GET,
                  jwt                     =%superq(JWT),
                  jsonOutFileNm           =_json_
                  );

  libname jsondata json fileref=_json_ ;

  proc copy in=jsondata out=WORK;
  run;

  proc sort data=items (drop=ordinal_root) out=_CI360HubFileList ;
    by id ;
  run ;

  proc sql noprint ;
    select id into: TableIDList separated by " " from items ;
  quit ;

  %let maxlen = 0 ;
  %do i = 1 %to %words(&TableIDList.) ;

    %let id = %scan(&TableIDList.,&i.,%str( )) ;
    %let url = &url_base./marketingData/tables/&id. ;

    ** for a table ID - can get its name and attributes **;

    filename tbl&i. TEMP ;
    %Call_Proc_HTTP(url                     =%str(&url.),                    
                    Method                  =GET,
                    JWT                     =%superq(JWT), 
                    jsonOutFileNm           =tbl&i.
                    );

    libname table&i. json fileref=tbl&i. ;

    data Alldata&i. ;
      set table&i..Alldata ;
    run ;

    proc copy in=table&i. out=work ;
    run;

    %let curlen = %varlen(Alldata&i.,value,IncludeDollarSign=N) ;
    %if %eval(&curlen. > &maxlen.) %then 
      %let maxlen = &curlen. ;

    proc transpose data=Alldata&i. (where=(P=1 AND p1 not in ("dataItems" "links"))) out=table_T&i. (drop=_name_ rename=(name=name2)) ;
      id p1 ;
      var value ; 
    run ;
      
    ** DATAITEMS dataset: has variable level metadata *;
    ** Add DATAITEMS_CUSTOMPROPERTIES custom properties to variable level data when it exists **;
    ** Add DATAITEMS_TAGS1 if exists to the variable level data **;
    
    %if %sysfunc(exist(DATAITEMS_CUSTOMPROPERTIES)) %then %do ;
      %let droplist = ord_dataitems ordinal_customProperties ;
    %end ;
    %else %do ;
      %let droplist =  ;
    %end ;

    proc sql noprint ;
      create table VarDetails&i. (drop=&droplist. ord_dataitems2 ordinal_tags ordinal_root) as
      select t1.*, "&id." as TableID
      %if %sysfunc(exist(DATAITEMS_CUSTOMPROPERTIES)) %then %do ;
        , t2.*
      %end ;
      , t3.*
      from dataitems (rename=(label=SASlabel)) as t1 
         %if %sysfunc(exist(DATAITEMS_CUSTOMPROPERTIES)) %then %do ;
         left join 
           DATAITEMS_CUSTOMPROPERTIES (rename=(ordinal_dataitems=ord_dataitems)) as t2
           on  t1.ordinal_dataitems=t2.ord_dataitems
         %end ;
         left join 
           DATAITEMS_TAGS (rename=(ordinal_dataitems=ord_dataitems2)) as t3
           on  t1.ordinal_dataitems=t3.ord_dataitems2

      order by t1.ordinal_dataitems ;
    quit;

    proc datasets library=WORK nolist ;
      delete DATAITEMS_CUSTOMPROPERTIES ;
    quit ;

  %end ;

  data TableAttr ;   
    length TableName $32. ID %varlen(_CI360HubFileList,id) ;
    set %do i=1 %to %words(&TableIDList.) ;
          table_T&i. (rename=(id=id_old)) 
        %end ;
    ;
    id = strip(id_old) ;
    If Nvalid(Name2) then TableName = strip(Name2) ;
    else TableName = substr(name2,index(name2,"(")+1,index(name2,")")-index(name2,"(")-1) ;
    if NOT Nvalid(tableName) then TableName = compress(strip(Name2));
    if NOT Nvalid(tableName) then abort ;
    drop Name2 id_old ;
  run ;

  proc sort data=TableAttr ; by tableName ; run ;
  
  data _null_ ;
    set tableAttr ;
    by TableName ;
    if not (first.tablename and last.tablename) then abort ;
  run ;

  proc sort data=tableAttr ;
    by id ;
  run ;

  data &outTableListds. (rename=(ID=TableID Name=TableLongName Description=TableDescription)) ;
    merge _CI360HubFileList (in=in1 drop=ordinal_items)
          TableAttr         (in=in2 drop=version)
    ;
    by id ;
    if not (first.id and last.id) then abort ;
  run ;

  proc sql noprint ;
    create table VarMetaData as
    select Name, type, max(length) as maxLen from sashelp.vcolumn
    where upcase(libname)="WORK" AND upcase(memname) in (%do i=1 %to %words(&TableIDList.) ; "VARDETAILS&I." %end ;) 
    group by Name, type ;
  quit;

  filename attrfix TEMP ;
  data _null_ ;
    set VarMetaData ;
    file attrfix ;
    if upcase(trim(left(type))) = "CHAR" then 
      put name "$" maxlen +(-1) "." ;
    else 
      put name maxlen +(-1) "." ;
  run ;

  %do i=1 %to %words(&TableIDList.) ;
    data VarDetails&i. ;
      length 
        %include attrfix ;  
      ;
      set VarDetails&i. ; 
    run ;
  %end ;

  data VarDetails0 ;   
    retain tableID Name ordinal_dataItems SASLabel Description type ;
    length Name $32. ;
    set %do i=1 %to %words(&TableIDList.) ;
          VarDetails&i.  
        %end ;
    ;
  run ;
 
  proc sort data=VarDetails0 ; by tableID ordinal_dataItems ; run ;

  data &outVarListDs. (rename=(Name=VarName SASLabel=VarLabel Description=VarDescription ordinal_dataItems=VarOrder type=VarType)) ;
    merge &outTableListds. (in=intable keep=TableID TableName)
          VarDetails0      (in=invar)
    ;
    by TableID ;
    if NOT (intable and invar) then abort ;

    Name = upcase(Name) ;
    if Name <= " " then do ;
      Name = SASLabel ;
      Put "Warning: For " TableName= " name is missing, set it to " saslabel= ;
    end ;
    if Name <= "" then abort ;
  run ;

  %let NumHubTables = %words(&TableIDList.)  ;

  %if (&NumHubTables. > 50) %then %do ;
    %put Note: There are &NumHubTables. HUB files to download but CI360 only allows 50 POST calls per day ;
    %put Note: Subset the list of hub tables to download to less than 50 files prior to calling the download macro ;    
  %end ;

  %FINISH:
          
%mend ;
