/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Read_UDM_Data.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2019 August
/ LastModBy : Noah Powers
/ LastModDt : 11.19.2019
/ Purpose   : Read the zip files downloaded from 360 Discover into SAS tables.
/ FuncOutput: N/A
/ Usage     :
/ Notes     : The Discover / Engage unification data has some new data types that need
/             to be mapped to thh appropriate SAS
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ Files2ReadDs  Name of the SAS dataset that contains the list of files to read that
/               was generated from the Download_Discover_Data macro
/ SchemaDs      Name of the SAS dataset that contains the schema for the tables 
/ raw_data_path Full path that contains all of the gzip files that were downloaded           
/ OutLib        SAS library name where the full data will be read and saved
/ EndFileMatch  Default is * to match all gz files in the raw data path but this can be used
/               to restrict with gz files are read into sas data
/ FilesUncompressed (Y/N) Set to Y if the files have been uncompressed and then the UNCOMPRESSCOMMAND 
/                   value will be ignored. 
/ UncompressCommand Default is %str(gzip -dc) but another option is %str(7z e -so). In addition, 
/                   users can now include &table. and &raw_data_path. as part of the uncompress 
/                   command and if this is done be sure to enclose the whole parameter in %nrstr().  
/                   For example, this code avoided arg list too long errors:
/                   %nrstr(find "&raw_data_path." -name "&table." -print0 | xargs -0 gzip -dc)
/                   Otherwise, the following gets added to the end of the uncompress command 
/                   "&raw_data_path.&slash.&table."
/                   If this parameter is explicitly passed as a blank/null
/                   value, then the ZIP filename is used to read each zip file separately 
/                   which takes significantly longer but will work without XCMD enabled
/============================================================================================*/
%macro Read_UDM_Data(Files2ReadDs   =,
                     SchemaDs       =,
                     TimeStampsDs   =,
                     raw_data_path  =,
                     outlib         =WORK,
                     EndfileMatch   =%str(*),
                     FilesUncompressed=N,
                     UncompressCommand=%str(gzip -dc)
                     ) ;

  %local tableList t table inputList slash obs zipfile quote ;

  %if (&sysscp. = WIN) %then %do;
    %let slash = %str(\);
    %let quote = %str(%");
  %end;
  %else %do;
    %let slash = %str(/);
    %let quote = %str() ;
  %end ;
  
  %if NOT %length(&FilesUncompressed.) %then %let FilesUncompressed = N ;
  %let FilesUncompressed = %upcase(%substr(&FilesUncompressed.,1,1)) ;

  proc sql noprint ;
    select distinct entityName into: tableList separated by " " from &Files2ReadDs. ;
  quit ;  

  proc sort data=&SchemaDs. ; by table_name column_sequence; run;

  %do t = 1 %to %words(&tableList.) ;
    %let table = %upcase(%scan(&tableList.,&t.,%str( ))) ;

    proc sql noprint ;
      select column_name into: inputList separated by " " from &SchemaDs. where upcase(table_name) = "&table" ;
    quit ; 

    filename attr TEMP ;
    ** create input and length statement from metadata **;
    data _null_ ;
      set &SchemaDs. (where=(upcase(table_name) = "&table")) end=lastrec ;
      file attr ;

      if _N_ = 1 then put @6 "attrib" ;
      if (data_type= 'varchar' or data_type= 'char') then 
        put @8 column_name 'FORMAT=$' data_length +(-1) '.';
      else if data_type = 'timestamp' then
        put @8 column_name 'LENGTH=8 FORMAT=DATETIME27.6 INFORMAT=ymddttm.';
      else if data_type = 'date' then 
        put @8 column_name 'LENGTH=8 FORMAT=MMDDYY10. INFORMAT=yymmdd10.';
      else if data_type in ('smallint' 'int' 'bigint' 'decimal' 'tinyint' 'double') then 
        put @8 column_name 'LENGTH=8.' ;
      else if data_type = "map" then 
        put @8 column_name 'FORMAT=$4000.' ;
      else if data_type = "" and column_type = "array<string>" then 
        put @8 column_name 'FORMAT=$32000.' ;
      else abort ;
      if lastrec then put @6 ';' ;
    run ;

    %if ("&FilesUncompressed." = "Y") %then %do ;
    
     filename text&t. %unquote(%nrbquote('&quote.&raw_data_path.&slash.&table._i&EndfileMatch.&quote.'));
      
      data &outlib..&table. (compress=YES);
        infile text&t. dlm = '01'x dsd MISSOVER LRECL=60000  ;
        %include attr ;
        input &inputList. ;
      run ;    
      
    %end ;
    %else %if %length(&UncompressCommand.) %then %do ;
    
      %let fullTable = %nrbquote(&table._i&EndfileMatch..gz) ;     
      
      %if %sysfunc(index(%superq(UncompressCommand),%nrstr(&table.))) > 0 %then %do ;
        filename zip&t. PIPE %unquote(%nrbquote('%replace(%superq(UncompressCommand),%nrstr(&table.),%nrstr(&fullTable.))'));
      %end ;
      %else %do ;
        filename zip&t. PIPE %unquote(%nrbquote('&UncompressCommand. &quote.&raw_data_path.&slash.&table._i&EndfileMatch..gz&quote.')) ;
      %end ;
      
      data &outlib..&table. (compress=YES);
        infile zip&t. dlm = '01'x dsd MISSOVER LRECL=60000  ;
        %include attr ;
        input &inputList. ;
      run ;    

    %end ;
    %else %do;
    
      data _files2Read&t._ (compress=YES) ;
        set &Files2ReadDs. (where=(entityname = "&table."));
      run ;

      proc datasets library=&outlib. nolist ;
        delete &table. / memtype=data ;
      quit ;

      %do obs = 1 %to %nobs(_files2Read&t._) ;
        data _null_ ;
          set _files2Read&t._ (obs=&obs.) ;
          call symput("zipfile",strip(path)) ;
        run ;

        filename disc ZIP "&zipfile." GZIP ;

        data _tmp_ (compress=YES);
          infile disc dlm = '01'x dsd MISSOVER LRECL=60000  ;
          %include attr ;
          input &inputList. ;
        run ;

        proc append data=_tmp_ base=&outlib..&table. (compress=YES) ;
        run ;

      %end ;
    %end ;
    
    %if %nobs(&outlib..&table.) = 0 %then %do ;
      proc datasets library=&outlib. nolist ;
        delete &table. ;
      quit ;
      
      %if %length(&TimeStampsDs.) %then %do ;
        data &TimeStampsDs. ;
          set &TimeStampsDs. ;
          if upcase(entityName) = "%upcase(&table.)" then delete ;
        run ;
      %end ;
    %end ;

  %end ;

  %FINISH:
%mend ;