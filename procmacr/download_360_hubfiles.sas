/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Download_360_HubFiles.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2019 November
/ LastModBy : Noah Powers
/ LastModDt : 11.22.2019
/ Purpose   : Create a SAS dataset wiht the list of files that have been uploaded to a particular 
/             tenant. What happens if a user has access to more than one tenant? 
/ FuncOutput: N/A
/ Usage     :
/ Notes     : The contact preferences dataset has blank values for variable names but the 
/             labels appear to be valid SAS names.  Need to add code to fill this in.
/
/             From defect S1555975 I learned two things:
/ 
/             1. There is a limit of 50 requests for the API in question;
/ 
/             In this case, the API limit for "dumpToS3" is 50 new requests per day (POST
/             only), which does not include the subsequent GET calls to check status or
/             download the data. We think 50 is in general sufficient for most of the tenants.
/             However, if a tenant really wants to exceed a certain limit with reasonable
/             justification, we can help overwrite the limit through a support ticket.
/
/ Steps:
/
/ Loop through each data file to download it by 
/    a. REST call to get the export links
/    b. determine the link with the header line only
/    c. REST call to save header .gz file 
/    d. loop through all remaining links and
/       i. REST call to save data .gz files 
/
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name      Description
/ -------------------------------------------------------------------------------------
/ JWT             Java Web Token based on the tenant id and secret key.  
/                 The %Gen_JWT() macro can create this for you.
/ url_base        Default is %nrstr(https://extapigwservice-demo.cidemo.sas.com) - change as appropriate for
/                 your tenant
/ TableListds     The dataset generated from the Get360HubFileMetaData macro with the list of 
/                 tables and thier IDs in the HUB
/ raw_data_path   The filepath location where the .gz files will be saved
/ OutHubFileList  The output dataset that links the .gz files to the HUB dataset that they 
/                 go with.  This is used by the Read360HubData to read these gz files into 
/                 SAS datasets
/============================================================================================*/

%macro Download_360_HubFiles(jwt           =,
                             url_base      =%nrstr(https://extapigwservice-demo.cidemo.sas.com),
                             TableListds   =,
                             raw_data_path =,
                             OutHubFileList=WORK.Hubfiles2Read
                             ) ;

  %local NumTables TableIDList i id url slash ;

  %if (&sysscp. = WIN) %then %do;
    %let slash = %str(\);
  %end;
  %else %do;
    %let slash = %str(/);
  %end ;

  %let NumTables = %nobs(&TableListds.) ;

  %if (&NumTables. > 50) %then %do ;
    %put Note: There are &NumTables. HUB files to download but CI360 only allows 50 POST calls per day ;
    %put Note: Subset the list of hub tables to download to less than 50 files prior to calling this download macro ;    
    %goto FINISH ; 
  %end ;
  
  proc sql noprint ;
    select Tableid into: TableIDList separated by " " from &TableListds. ;
  quit ;

  proc datasets library=work nolist ;
    delete _Hubfiles2Read_ / memtype=data ;
  quit;

  %do i = 1 %to %words(&TableIDList.) ;

    %let id = %scan(&TableIDList.,&i.,%str( )) ;
    %let url = &url_base./marketingData/tableJobs ;

    filename json_in TEMP ;

    data _null_ ;
      file json_in ;
      put "{" ;
      put '  "jobType":"TABLE_DOWNLOAD",' ;
      put '  "dataDescriptorId":"' "&id." '"' ;
      put "}" ;
    run ;

    ** for each table ID can request export links **;
    filename tblUrls TEMP ;
    %Call_Proc_HTTP(url                     =%str(&url.),                    
                    InFile                  =json_in,                    
                    Method                  =POST,
                    jwt                     =%superq(jwt),
                    jsonOutFileNm           =tblUrls
                    );

    libname links json fileref=tblUrls ;

    filename script TEMP ;
    data hub_files ;
      length path url $10000 file_ext $3. ;
      set links.DownloadItemList (drop=ordinal_root);
      length filename $100. filepath $20000 TableID %varlen(&TableListds.,Tableid) header 3. ;

      TableNum  = &i. ;   
      TableID = "&id." ;

      if index(path,".csv.part") > 0 then file_ext = "csv" ;
      else if index(path,".gz.part") > 0 then file_ext = "gz" ;
      else abort ;  

      if index(path,"/header_") > 0 then do ;
        header = 1 ;
        filename = "HUB&i.header." || file_ext ;
      end ;
      else do ;
        header = 0 ;
        filename = "HUB&i.part" || strip(put(_N_,8.)) || "." || file_ext ;
      end ;
      filepath = "&raw_data_path.&slash." || trim(left(filename)) ;

      file script ;
      put 'filename hub' _N_ "'" filepath +(-1) "';" ;
      put '%Call_Proc_HTTP(url =%nrstr(' url +(-1) '),jsonOutFileNm=hub' _N_ ');' ;
    run ;

    ** Download the data **; 
    %include script ;

    proc append data=hub_files base=_Hubfiles2Read_ ;
    run ;

  %end ;

  proc means data=_Hubfiles2Read_ nway noprint ;
    class Tableid ;
    var header ;
    output out=chk_files (drop=_type_ _freq_) sum(header)=header_sum ;
  run ;

  data _null_ ;
    set chk_files ;
    if header_sum NE 1 then abort ;
  run ;

  proc freq data=_Hubfiles2Read_ ;
    tables file_ext / missing out=ext_list ;
  run ;

  %if (%nobs(ext_list) > 1) %then %do ;
    %put Warning: Downloaded files do NOT all have the same type/file extension ;
  %end ;

  data &OutHubFileList. ;
    set _Hubfiles2Read_ ;
  run ;

  %FINISH:
%mend ;
