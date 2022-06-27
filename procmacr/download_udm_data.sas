/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Download_UDM_Data.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2019 August
/ LastModBy : Noah Powers
/ LastModDt : 11.19.2019
/ Purpose   : Automate the process of downloading the gzipped Discover data csv files.  The 
/             user chooses between three types of data:
/             - detail (business processes, goals, product views etc)
/             - identity (not time based - three tables)
/             - dbtTables (tables behind the 360 UI reports
/ FuncOutput: N/A
/ Usage     :
/ Notes     :  See the CI360 user manual for full details on how to download the data
/                https://go.documentation.sas.com/?cdcId=cintcdc&cdcVersion=production.a&docsetId=cintag&docsetTarget=extapi-discover-service.htm&locale=en#n1dcw035ykd0xwn1mp0yczqroca6
/
/              To test/see the unified discover/engage tables use these parms:
/              schemaVersion=4
/ 
/               gives you the unified tables.  
/               You also need to specify the category= parameter. 
/               If you don’t you only get the Discover tables (default).
/               Possible categories and tables included are listed here:  
/               http://sww.sas.com/saspedia/CI_360_Subject_Area_Category_and_Licensing 
/
/               License          Subject Area Category
/               Discover        DISCOVER
/               Engage Digital  ENGAGEDIGITAL, ENGAGEWEB, ENGAGEMOBILE, ENGAGEMETADATA, CDM
/               Engage Direct   ENGAGEDIRECT, CDM, ENGAGEMETADATA
/               Engage Email    ENGAGEEMAIL, ENGAGEMETADATA, CDM
/               Plan            PLAN
/
/              If sub-hourly time periods are desired, the TESTMODEPARMS macro paramter
/              can be used with a value such as %nrstr(&subHourlyDataRangeInMinutes=10) to 
/              add this parameter to the API call URL string.
/ 
/  Need to detect the case where the API returns links to data but the user requested 
/  table(s) have to data and exit gracefully
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ JWT                     Json Web Token based on the tenant id and secret key.  
/                         The %Gen_JWT() macro can create this for you.
/ mart_name               Per the API instructions, This is expected to be one of three values:
/                         IDENTITY, DBTREPORT or DETAIL 
/ Tables                  (optional) If it is desired to limit the tables downloaded, provide
/                         a space delimited list of table names to INCLUDE/DONWLOAD data for. 
/ ExtGatewayAddress       The external gateway address for the tenant.  This can be found in the 
/                         CI360 UI at General | External | Access. 
/ agent_name              The default value is %str(DiscoverAgent)
/ testModeParms           List exactly as they should show up on the URL as paramters.  See above
/                         in notes for how to use this macro parameter
/ raw_data_path           Path where the raw gzipped files will be stored
/ start_hour              (optional) This is used when the dataRange parameters below are not used.
/ end_hour                (optional) This is used when the dataRange parameters below are not used.
/ limit                   default value of 20. 
/ dataRangeStartTimeStamp Use the format 2019-11-01T00:00:00.000Z to specify start timestamp for data
/ dataRangeEndTimeStamp   Use the format 2019-11-01T00:00:00.000Z to specify end timestamp for data
/ RenameExistingGZ        Default=Y which triggers an OS file rename of all existing *.gz files in 
/                         the raw data path.  This is useful when multiple DownloadDiscoverData macro
/                         calls will be made over different time periods with the same raw data path
/                         folder.
/ FileTag                 (optional) If this is provided, the FileTag value is added to the end of
/                         filenames for each of the  *.gz files downloaded from 360 cloud.  This can
/                         help to create unique filenames across multiple DownLoadDiscoverData macro
/                         invocations.
/ OutFiles2ReadNm         (default =Files2Read) This is the name of the SAS dataset this macro 
/                         creates to store the list of downloaded gz files and associated metadata
/ OutTimeStampsNm         (default=TimeStamps) This is the name of the SAS dataset this macro 
/                         creates to store the list of entities it downloaded along with the 
/                         start and end dates of data downloaded
/ OutSchemaNm             (default=Schema)This is the name of the dataset that holds the schema or
/                         table-column level metadata that will be needed in order to read the 
/                         downloaded gz files into SAS datasets
/ OutIterFilePath         If this is not specified, then the underlying path associated with OUTLIB
/                         is used.  This is where files useful for debugging are stored related
/                         to the iterations of the download process... JSON files etc
/ Outlib                  The name of the sas library where the list of files to read and schema files 
/                         will be saved.
/============================================================================================*/
%macro Download_UDM_Data(JWT                     =,
                         mart_name               =,
                         Tables                  =,
                         ExtGatewayAddress       =,                      
                         agent_name              =%str(DiscoverAgent),
                         testModeParms           =%nrstr(&schemaVersion=4&category=DISCOVER&includeAllHourStatus=true),
                         raw_data_path           =,
                         start_hour              =,
                         end_hour                =,
                         limit                   =,
                         dataRangeStartTimeStamp =,
                         dataRangeEndTimeStamp   =,
                         RenameExistingGZ        =Y,
                         FileTag                 =,
                         OutFiles2ReadNm         =Files2Read,
                         OutTimeStampsNm         =TimeStamps,
                         OutSchemaNm             =Schema,
                         OutIterFilePath         =,
                         outlib                  =WORK) ;

  %local download_url_base dsc_download_url base_url more_time_periods iter cur_start i dt_str 
         PathLen schemaVersion slash ;

  %if (&sysscp. = WIN) %then %do;
    %let slash = %str(\);
  %end;
  %else %do;
    %let slash = %str(/);
  %end ;

  %if NOT %length(&limit.) %then 
    %let limit = 20 ;
  %if NOT %length(&start_hour.) %then 
    %let start_hour = 0 ;
  %if NOT %length(&RenameExistingGZ.) %then 
    %let RenameExistingGZ = Y ;
  %let RenameExistingGZ = %upcase(%substr(&RenameExistingGZ.,1,1)) ;
  %if NOT %length(&OutIterFilePath.) %then 
    %let OutIterFilePath = %sysfunc(pathname(&outlib.)) ;

  %let download_url_base = %str(&ExtGatewayAddress./discoverService/dataDownload/eventData/) ;
  %let PathLen = %sysevalf(%length(&OutIterFilePath.) + 255) ;

  %if "%upcase(&mart_name.)" = "IDENTITY" %then %do ;
    %let dsc_download_url = &download_url_base.%str(detail/nonPartitionedData)%nrstr(?agentName=)%str(&AGENT_NAME.) ;
  %end ;
  %else %if "%upcase(&mart_name.)" = "DBTREPORT" %then %do ;
    %let mart_name = dbtReport ;
    %let dsc_download_url = &download_url_base.&mart_name.%nrstr(?agentName=)%str(&AGENT_NAME.) ;
  %end ;
  %else %if "%upcase(&mart_name.)" = "DETAIL" %then %do ;
    %let dsc_download_url = &download_url_base.%lowcase(&mart_name.)/partitionedData%nrstr(?agentName=)%str(&AGENT_NAME.) ;
  %end ;
  %else %do ;
    %put E%upcase(rror): mart_name (&mart_name.) must be one of the following: identity, dbtReport or Detail ;
    %goto FINISH ;
  %end ;

  %let base_url = &dsc_download_url. ;
  %let more_time_periods = 1 ;
  %let iter = 0 ;
  %let cur_start = &start_hour. ;
  %let Outlib_path = %sysfunc(pathname(&outlib.));

  data _null_ ;
    length dt_str $19. ;
    dt_str = translate(put(datetime(),datetime19.),"_",":") ;
    call symputx("dt_str",strip(dt_str)) ;
  run ;
  
  %if NOT %length(&FileTag.) %then %let FileTag = _&dt_str. ;

  proc datasets library=work nolist ;
    delete files2Read schema_url_list / memtype=data ;
  quit;

  %if ("&RenameExistingGZ." = "Y") %then %do ;
    %put NOTE: Any existing .gz files in &raw_data_path. will be renamed to have a .gz_ignore extension so they will NOT be read in ;

    %if &slash. = %str(\) %then %do ;
      data _null_;
        call system (%unquote(%nrbquote('rename "&raw_data_path.&slash.*.gz" *.gz_ignore')));
      run;
    %end ;
    %else %do ;
      data _null_;
        call system (%unquote(%nrbquote('mv &raw_data_path.&slash.*.gz &raw_data_path.&slash.*.gz_ignore')));
      run;
    %end ;
  %end ;

  ** Loop through all hours and download zip files **;
  %do %while (%eval(&more_time_periods.)) ;

    %let iter = %eval(&iter. + 1) ;
  
    %let dsc_download_url = &base_url. ;
    %if "&cur_start." ne "" %then %do ; 
      %let dsc_download_url = &dsc_download_url.%nrstr(&start=)%str(&cur_start.) ;
    %end ;
    %if "&limit." ne "" %then %do ; 
      %let dsc_download_url = &dsc_download_url.%nrstr(&limit=)%str(&limit.) ;
    %end ;
    %if "%upcase(&mart_name.)" NE "IDENTITY" %then %do ;
      %if "&dataRangeStartTimeStamp." ne "" %then %do ; 
        %let dsc_download_url = &dsc_download_url.%nrstr(&dataRangeStartTimeStamp=)%str(&dataRangeStartTimeStamp.) ;
      %end ;
      %if "&dataRangeEndTimeStamp." ne "" %then %do ; 
        %let dsc_download_url = &dsc_download_url.%nrstr(&dataRangeEndTimeStamp=)%str(&dataRangeEndTimeStamp.) ;
      %end ;
    %end ;
    %if "%superq(testModeParms)" ne "" %then %do ;
      %let dsc_download_url = &dsc_download_url.%superq(testmodeparms) ;
    %end ;

    filename urllist "&OutIterFilePath.&slash.JsonOut&FileTag..txt" ;
    %Call_Proc_HTTP(url            =%superq(dsc_download_url),
                    jwt            =&JWT.,
                    headerList     =%str("Accept" = "application/vnd.sas.collection+json"),
                    jsonOutFileNm  =urllist) ;

    libname jsondata json fileref=urllist ; 

    proc copy in=jsondata out=work;
    run;

    proc datasets library=work nolist ;
      delete ALLDATA&iter.
             ENTITIES_DATAURLDETAILS&iter.
             ITEMS&iter.
             ITEMS_ENTITIES&iter.
             LINKS&iter.
             root_data&iter.
      ;
      change ALLDATA=ALLDATA&iter. 
             %if %sysfunc(exist(ENTITIES_DATAURLDETAILS)) %then %do ;
               ENTITIES_DATAURLDETAILS=ENTITIES_DATAURLDETAILS&iter.
             %end ;
             ITEMS=ITEMS&iter.
             ROOT=root_data&iter.
             %if %sysfunc(exist(ITEMS_ENTITIES)) %then %do ;
               ITEMS_ENTITIES=ITEMS_ENTITIES&iter.
             %end ;
             %if %sysfunc(exist(LINKS)) %then %do ;
               LINKS=LINKS&iter.
             %end ;
      ;
    quit ;

    filename urllist CLEAR ;

    %if (%eval(&iter.=1)) %then %do ;
      ** The count value in root does not change over iterations **;
      proc sql noprint ;
         select count as Total_NoOfRanges into :Total_NoOfRanges from root_data&iter. ;
      quit ;

      %put Total Number of hourly Time Ranges to Process: &Total_NoOfRanges.;
    %end ;

    %if (NOT %sysfunc(exist(ITEMS_ENTITIES&iter.)) OR NOT %sysfunc(exist(ENTITIES_DATAURLDETAILS&iter.))) %then %goto NODATA ;

    proc sql noprint ;
      create table download_details&iter. as
      select t1.ordinal_items as range_id, 
             %if %hasvars(items&iter.,dataRangeStartTimeStamp dataRangeEndTimeStamp) %then %do ;
               dataRangeStartTimeStamp, 
               dataRangeEndTimeStamp, 
             %end ;
             %if %hasvars(items&iter.,dataRangeProcessingStatus) %then %do ;
               dataRangeProcessingStatus,
             %end ;
             SchemaUrl,
             SchemaVersion,
             entityName,
             ordinal_dataUrlDetails as url_id, 
             url
      from items&iter. as t1 
         inner join 
           items_entities&iter. as t2
           on  t1.ordinal_items=t2.ordinal_items
         inner join 
           entities_dataurldetails&iter. t3
           on  t2.ordinal_entities=t3.ordinal_entities

      %if %length(&Tables.) %then %do ;
        where upcase(strip(entityname)) in (%quotelst(%upcase(&Tables.))) 
      %end ;

      order by t1.ordinal_items ,entityName ;

      select count(distinct(range_id)) into :NoOfItems from download_details&iter. ;
    quit;

    %if (NOT %nobs(download_details&iter.)) %then %goto NODATA ;

    filename script "&OutIterFilePath.&slash.download_&FileTag._&iter..sas" ;
    data dsc_files ;
      length schemaUrl url $5000. entityName $32. path $&PathLen.. ;
      set download_details&iter. ;
      by range_id ;
      retain part_no iteration ;

      entityname = upcase(entityname) ;
      iteration = &iter. ;
      if first.range_id then part_no = 0 ;
      part_no = part_no + 1 ;
      path = "&raw_data_path.&slash." || strip(entityname) ||  "_i&iter._r" || strip(put(range_id,8.)) || "_p" || 
              strip(put(part_no,8.)) || "%trim(&FileTag.).gz" ;

      file script ;
      put 'filename dsc' _N_ "'" path +(-1) "';" ;
      put '%Call_Proc_HTTP(jwt=,url=%nrstr(' url +(-1) '),jsonOutFileNm=dsc' _N_ ');' ;
    run ;

    proc append data=dsc_files base=files2Read FORCE ;
    run ;
    
    %include script ;

    proc sort NODUPKEY data=download_details&iter. 
      out=schema_url_list&iter. (keep=schemaURL schemaVersion); 
      by schemaUrl ; 
    run ;

    proc append data=schema_url_list&iter. base=schema_url_list FORCE ;
    run ;

    %NODATA:

    %if NOT %length(&end_hour.) %then %do ;
      data _null_ ;
        set root_data&iter. ;
        call symputx("end_hour",put(count,8.)) ;
      run ;
    %end ;

    ** Update start time **; 
    %put Iteration:&iter. Complete. start=&cur_start. end=%eval(&cur_start. + &limit. -1) limit=&limit. ;
    data _null_ ;
      set root_data&iter. ;
      new_start = &cur_start. + limit ;
      if new_start > min(count,&end_hour.) then 
        call symputx("more_time_periods",0) ;
      call symputx("cur_start",strip(put(new_start,8.))) ;
      file LOG ;
      new_end_hour = min(count,&end_hour.,new_start + limit - 1) ;
      new_limit = new_end_hour - new_start + 1 ;
      call symput("limit",strip(put(new_limit,8.))) ;
    run ;

    %if %eval(&more_time_periods.) %then 
      %put New Values: Cur_start = &cur_start. Last Hour=%eval(&cur_start. + &limit. -1) Limit = &limit. ;

  %end ;

  %if %sysfunc(exist(schema_url_list)) %then %do ;

    proc sort NODUPKEY data=schema_url_list ; 
      by schemaURL ;
    run ;

    proc sql noprint ;
      select schemaUrl into: schemaUrl from Schema_Url_list (obs=1) ;
    quit ;

    filename _json_  "&OutIterFilePath.&slash.JsonSchemaOut_&FileTag._&iter..txt" ;
    %Call_Proc_HTTP(url            =%superq(schemaUrl),
                    jwt            =,                 
                    jsonOutFileNm  =_json_) ;

    libname mdjson json fileref=_json_;
    proc copy in=mdjson out=work;
    run;

    proc datasets library=work nolist ;
      delete _schema_ ;
      change ROOT=_schema_ ;
      /** this has vars for ALL tables.  Not just those in the tables macro parameter **/
    quit ;

    data &outlib..&OutSchemaNm. ;
      set _schema_ ;
    run ;

  %end ;
  %else %do ;
    data &outlib..&OutSchemaNm. ;
      length 
        ordinal_root 8.
        table_name $24.
        column_name $29.
        Column_label $29.
        column_sequence 8.
        data_type $9.
        data_length 8.
        column_type $13.
        ;
      delete ;
    run ;
  %end ;

  %if NOT %sysfunc(exist(files2Read)) %then %do ;
    data files2Read ;
      length 
        schemaUrl $5000.
        url $5000.
        range_id 8.
        dataRangeStartTimeStamp $24.
        dataRangeEndTimeStamp $24.
        schemaVersion $3.
        entityName $32.
        url_id 8.
        path $&PathLen..
        part_no 8.
        iteration 8.
        dataRangeStart_dt 8.
        dataRangeEnd_dt 8.
      ;
      format dataRangeStart_dt dataRangeEnd_dt DATETIME27. ;
      delete ;
    run ;
  %end ;

  proc sql noprint ;
    select distinct schemaVersion into: schemaVersion separated by " " from files2read ;
  quit ;

  %if "%upcase(&mart_name.)" = "IDENTITY" %then %do ;
    data _TimeStamps_ ;
      length schemaVersion $3. ;
      now = datetime() ;
      dataRangeStart_dt = now ;
      dataRangeEnd_dt = now ;
      schemaVersion = "&schemaVersion." ;
      FORMAT dataRangeStart_dt dataRangeEnd_dt DATETIME27. ;
      drop now ;
    run ;

    data &outlib..&OutFiles2ReadNm. ;
      set files2Read ;
      now = datetime() ;
      dataRangeStart_dt = now ;
      dataRangeEnd_dt = now ;
      FORMAT dataRangeStart_dt dataRangeEnd_dt DATETIME27. ;
      drop schemaUrl url schemaVersion now ;
    run ;
  %end ;
  %else %do ;
    data &outlib..&OutFiles2ReadNm. (compress=YES) ;
      set files2Read ;
      dataRangeStart_dt = input(dataRangeStartTimeStamp,ymddttm.) ;
      dataRangeEnd_dt = input(dataRangeEndTimeStamp,ymddttm.) ;
      FORMAT dataRangeStart_dt dataRangeEnd_dt DATETIME27. ;
      drop schemaUrl url dataRangeStartTimeStamp dataRangeEndTimeStamp schemaVersion ;
    run ;

    proc sort data=&outlib..&OutFiles2ReadNm. ;
      by EntityName dataRangeStart_dt dataRangeEnd_dt ;
    run ;

   
    proc sql noprint ;
      select max(dataRangeEndTimeStamp) into :Max_End_dt from files2Read ;
    quit ;

    data _TimeStamps_ ;
      length schemaVersion $3. ;
      dataRangeStart_dt = input("&dataRangeStartTimeStamp.",ymddttm.) ;
      dataRangeEnd_dt = input("&Max_End_dt.",ymddttm.) ;
      schemaVersion = "&schemaVersion." ;
      FORMAT dataRangeStart_dt dataRangeEnd_dt DATETIME27. ;
    run ;
  %end ;  
  
  proc sort NODUPKEY data=&outlib..&OutFiles2ReadNm. (keep=EntityName) out=EntityList ; 
    by EntityName ;
  run ;
  
  proc sql noprint ;
    create table &outlib..&OutTimeStampsNm. as select * from EntityList, _TimeStamps_ 
    order by entityName ;
  quit ;

  %FINISH:
%mend ;