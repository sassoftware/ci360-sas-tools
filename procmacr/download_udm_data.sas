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
/             user chooses between four marts of data:
/             - detail (business processes, goals, product views etc)
/             - snapshot (not time based - identity and metadata tables)
/             - dbtTables (tables behind the 360 UI reports
/             - cdm (common data model for direct)
/ FuncOutput: N/A
/ Usage     :
/ Notes     :  By default, an item is a single hour range (60 min interval) but if the subHourlyDataRangeInMinutes 
/              optional API parameter is used, then an item how ever many minutes are specified in that parameter.
/              But the same set of gzip files are provided either way - so not sure what the value of using
/              the subhourly parameter is.
/
/              See the CI360 user manual for full details on how to download the data
/              https://go.documentation.sas.com/?cdcId=cintcdc&cdcVersion=production.a&docsetId=cintag&docsetTarget=extapi-discover-service.htm&locale=en#n1dcw035ykd0xwn1mp0yczqroca6
/
/              You also need to specify the category= parameter. 
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
/              If sub-hourly time periods are desired, the AddlAPIParms macro paramter
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
/ ExtGatewayAddress       The external gateway address for the tenant.  This can be found in the 
/                         CI360 UI at General Settings | External Access | Access Points. 
/                         Copy this value exactly as it is from the UI.
/                         For example -> extapigwservice-eu-prod.ci360.sas.com
/ agent_name              The default value is %str(DiscoverAgent).  This comes from CI360 UI
/                         under General Settings | External Access | Access Points.
/ mart_name               Per the API instructions, This is expected to be one of four values:
/                         CDM, SNAPSHOT, DBTREPORT or DETAIL.  The IDENTITY value is depreciated,
/                         use SNAPSHOT instead 
/ SchemaVersion           The schema version of the mart to download
/ Category                The category of data to download.  See user manual for all values but
/                         this includes DISCOVER, ENGAGE, etc 
/ AddlAPIParms            List exactly as they should show up on the URL as paramters.  See above
/                         in notes for how to use this macro parameter
/ Tables                  (optional) If it is desired to limit the tables downloaded, provide
/                         a space delimited list of table names to INCLUDE/DONWLOAD data for. 
/ raw_data_path           Path where the raw gzipped files will be stored
/ limit                   This is the max number of items to return from a single API call. 
/                         The default value is 20.  There can be multiple entities with data for each
/                         item and even multiple gzip files for a give item-entity (hour-table). 
/ start_item_num          (optional) If specified, only items in the list at or above this number are downloaded
/ end_item_num            (optional) If specified, only items in the list at or below this number are downloaded
/ proxy_host              (optional) specifies the Internet host name of an HTTP proxy server.
/ proxy_port              (optional) specifies an HTTP proxy server port.
/ proxy_user              (optional) user name to use with proxy server
/ proxy_pwd               (optional) password to use with proxy server
/ dataRangeStartTimeStamp Use the format 2019-11-01T00:00:00.000Z to specify start timestamp for data
/ dataRangeEndTimeStamp   Use the format 2019-11-01T00:00:00.000Z to specify end timestamp for data
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
                         ExtGatewayAddress       =,      
                         agent_name              =%str(DiscoverAgent),
                         mart_name               =detail,                                                                                       
                         SchemaVersion           =9,
                         Category                =DISCOVER,                    
                         AddlAPIParms            =%nrstr(&includeAllHourStatus=true),
                         Tables                  =,
                         raw_data_path           =,                         
                         limit                   =20,
                         start_item_num          =,
                         end_item_num            =,
                         proxy_host              =,
                         proxy_port              =, 
                         proxy_user              =,
                         proxy_pwd               =,
                         dataRangeStartTimeStamp =,
                         dataRangeEndTimeStamp   =,                        
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
    
  %if NOT %length(&start_item_num.) %then 
    %let start_item_num = 0 ;
  
  %if NOT %length(&OutIterFilePath.) %then 
    %let OutIterFilePath = %sysfunc(pathname(&outlib.)) ;

  %let download_url_base = %str(https://&ExtGatewayAddress./marketingGateway/discoverService/dataDownload/eventData/) ;
  %let PathLen = %sysevalf(%length(&OutIterFilePath.) + 255) ;

  %if "%upcase(&mart_name.)" = "SNAPSHOT" %then %do ;
    %let dsc_download_url = &download_url_base.%str(detail/nonPartitionedData)%nrstr(?agentName=)%str(&AGENT_NAME.) ;
  %end ;
  %else %if "%upcase(&mart_name.)" = "DBTREPORT" %then %do ;
    %let mart_name = dbtReport ;
    %let dsc_download_url = &download_url_base.&mart_name.%nrstr(?agentName=)%str(&AGENT_NAME.) ;
  %end ;
  %else %if ("%upcase(&mart_name.)" = "DETAIL") OR ("%upcase(&mart_name.)" = "CDM") %then %do ;
    %let dsc_download_url = &download_url_base.detail/partitionedData%nrstr(?agentName=)%str(&AGENT_NAME.) ;
  %end ;
  %else %do ;
    %put E%upcase(rror): mart_name (&mart_name.) must be one of the following: snapshot, cdm, dbtReport or Detail ;
    %goto FINISH ;
  %end ;

  %let base_url = &dsc_download_url. ;
  %let more_time_periods = 1 ;
  %let iter = 0 ;
  %let cur_start = &Start_item_num. ;
  %let Outlib_path = %sysfunc(pathname(&outlib.));

  data _null_ ;
    length dt_str $19. ;
    dt_str = translate(put(datetime(),datetime19.),"_",":") ;
    call symputx("dt_str",strip(dt_str)) ;
  run ;

  proc datasets library=work nolist ;
    delete files2Read schema_url_list / memtype=data ;
  quit;

  ** Loop through all hours and download zip files **;
  %do %while (%eval(&more_time_periods.)) ;

    %let iter = %eval(&iter. + 1) ;
  
    %let dsc_download_url = &base_url.%nrstr(&schemaVersion=)&SchemaVersion.%nrstr(&category=)&category.  ;
    %if "&cur_start." ne "" %then %do ; 
      %let dsc_download_url = &dsc_download_url.%nrstr(&start=)%str(&cur_start.) ;
    %end ;
    %if "&limit." ne "" %then %do ; 
      %let dsc_download_url = &dsc_download_url.%nrstr(&limit=)%str(&limit.) ;
    %end ;
    %if "%upcase(&mart_name.)" NE "SNAPSHOT" %then %do ;
      %if "&dataRangeStartTimeStamp." ne "" %then %do ; 
        %let dsc_download_url = &dsc_download_url.%nrstr(&dataRangeStartTimeStamp=)%str(&dataRangeStartTimeStamp.) ;
      %end ;
      %if "&dataRangeEndTimeStamp." ne "" %then %do ; 
        %let dsc_download_url = &dsc_download_url.%nrstr(&dataRangeEndTimeStamp=)%str(&dataRangeEndTimeStamp.) ;
      %end ;
    %end ;
    %if "%superq(AddlAPIParms)" ne "" %then %do ;
      %let dsc_download_url = &dsc_download_url.%superq(AddlAPIParms) ;
    %end ;

    filename urllist temp ;
    %Call_Proc_HTTP(url            =%superq(dsc_download_url),
                    jwt            =&JWT.,
                    headerList     =%str("Accept" = "application/vnd.sas.collection+json"),
                    proxy_host     =&proxy_host.,
                    proxy_port     =&proxy_port., 
                    proxy_user     =&proxy_user.,
                    proxy_pwd      =&proxy_pwd.,
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
         select count into :end_item_num from root_data1 ;
      quit ;     

      %put Total Number of items (typically hourly Time Ranges) to process: &end_item_num.;
      
      %if NOT %length(&end_item_num.) %then %do ;
        data _null_ ;
          set root_data1 ;
          call symputx("end_item_num",put(count,8.)) ;
        run ;
      %end ;
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
             %if %hasvars(entities_dataurldetails&iter.,lastModifiedTimestamp) %then %do ;
               t3.lastModifiedTimestamp,
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

    filename script temp ;
    data dsc_files ;
      length schemaUrl url $5000. entityName $32. datekey 8. url_datekey $10. url_subhour $2.;
      set download_details&iter. ;
      %if %hasvars(download_details&iter.,dataRangeStartTimeStamp) %then %do ;         
        datekey= input(strip(put(datepart(input(dataRangeStartTimeStamp,ymddttm.)) , yymmddn8.)) || 
                       strip(put(hour(input(dataRangeStartTimeStamp,ymddttm.)),z2.)),16.) ;                       
        url_datekey = substr(url,index(url,"datekey%3D")+10,10) ;
        url_subhour = substr(url,index(url,"datekey%3D")+21,2) ;                       
      %end ;
      entityname = upcase(entityname) ;
      iteration = &iter. ;
    run ;
    
    proc sort data=dsc_files ; by entityname url_datekey url_subhour ; run ;
    
    data dsc_files ;
      set dsc_files ;
      length path $&PathLen.. end_file $100. ;
      by entityname url_datekey url_subhour ;
      retain part_no ;
      if first.url_subhour then part_no = 0 ;
      part_no = part_no + 1 ;
      %if "%upcase(&category.)" NE "CDM" %then %do ;
        date_hour = compress(strip(url_datekey) || "_" || strip(url_subhour)) ;
        if date_hour = "_" then end_file = "" ;
        else end_file = "_" || strip(date_hour) || "_p" || strip(put(part_no,8.)) ;
      %end ;
      %else %do ;
        date_hour = compress(strip(url_datekey)) ;
        if date_hour = "" then end_file = "" ;
        else end_file = "_" || strip(date_hour) || "_p" || strip(put(part_no,8.)) ;
      %end ;
      path = "&raw_data_path.&slash." || strip(entityname) || strip(end_file) || ".gz" ;

      file script ;
      put 'filename dsc' _N_ "'" path +(-1) "';" ;
      put '%Call_Proc_HTTP(jwt=,url=%nrstr(' url +(-1) "),proxy_host=&proxy_host.,proxy_port=&proxy_port.,proxy_user=&proxy_user.,proxy_pwd=&proxy_pwd.,jsonOutFileNm=dsc" _N_ ');' ;
      drop end_file date_hour ;
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

    ** Update start time **; 
    %put Iteration:&iter. Complete. start=&cur_start. end=%eval(&cur_start. + &limit. -1) limit=&limit. ;
    data _null_ ;
      set root_data&iter. ;
      new_start = &cur_start. + limit ;
      if new_start > min(count,&end_item_num.) then 
        call symputx("more_time_periods",0) ;
      call symputx("cur_start",strip(put(new_start,8.))) ;
      file LOG ;
      new_end_item_num = min(count,&end_item_num.,new_start + limit - 1) ;
    run ;

    %if %eval(&more_time_periods.) %then 
      %put New Values: Cur_start = &cur_start. Last Item Num=%eval(&cur_start. + &limit. -1) Limit = &limit. ;

  %end ;

  %if %sysfunc(exist(schema_url_list)) %then %do ;

    proc sort NODUPKEY data=schema_url_list ; 
      by schemaURL ;
    run ;

    proc sql noprint ;
      select schemaUrl into: schemaUrl from Schema_Url_list (obs=1) ;
    quit ;

    filename _json_  temp ;
    %Call_Proc_HTTP(url            =%superq(schemaUrl),
                    jwt            =, 
                    proxy_host     =&proxy_host.,
                    proxy_port     =&proxy_port., 
                    proxy_user     =&proxy_user.,
                    proxy_pwd      =&proxy_pwd.,
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

  %if "%upcase(&mart_name.)" = "SNAPSHOT" %then %do ;
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
      drop schemaUrl url schemaVersion now url_datekey range_id url_id iteration url_subhour ;
    run ;
  %end ;
  %else %do ;
    data &outlib..&OutFiles2ReadNm. (compress=YES) ;
      set files2Read ;
      dataRangeStart_dt = input(dataRangeStartTimeStamp,ymddttm.) ;
      dataRangeEnd_dt = input(dataRangeEndTimeStamp,ymddttm.) ;
      %if "%upcase(&category.)" = "CDM" %then %do ;
        drop url_subhour ;
      %end ;
      %else %do ;
        rename url_subhour = subHour;
      %end ;
      FORMAT dataRangeStart_dt dataRangeEnd_dt DATETIME27. ;
      drop schemaUrl url dataRangeStartTimeStamp dataRangeEndTimeStamp schemaVersion url_datekey range_id url_id iteration ;
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