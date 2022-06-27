/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : read_360hub_data.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2019 August
/ LastModBy : Noah Powers
/ LastModDt : 11.19.2019
/ Purpose   : Read the zip files downloaded from 360 Hub into SAS tables.
/ FuncOutput: N/A
/ Usage     :
/ Notes     :  
/  For each hub dataset, do the following :
/   1. Identify the zip file that contains the header row and read these values in order
/       and are expected to always have composite_user_id as the first column and identity_id as 
/       the last column e.g.
/
/       composite_user_id,ddf81dca22124ce488a7f83ef5e996,d5d7ce84c0884fd5b73385bbc1d2e5,identity_id
/  
/   2. Compare these with the corresponding columns in the HubVarListDs. which is expected to have
/      a subset of the columns from the header.  Make note of any additional columns not
/      already mentioned in 1 as these will be read in as max length character type sas variables
/ 
/   3. Use the information gathered in 2. to create the input statement with variables in the correct
/      order as well as the length statement to specify both type and bytes allocated to each.
/
/   4. Create the filename file list that includes all of the non-header .gz files associated with the 
/      current hub dataset being processed.  Read in the data as the ASCII text output of unnmaed 
/      PIPE from gzip.exe.  Note that data will have additional columns ||subject_id| at the
/      beginning of each row that will need to be ignored:
/
/    ||subject_id|67608,67608,t,e6da93f5-ad81-32a2-a4a0-1d00104ccffe
/    ||subject_id|97165,97165,t,5fbd74ce-a695-3d9c-9979-7395dd694cda
/    ||subject_id|38634,38634,t,b6650247-9d0e-3757-876d-5ca1f8ddad4e
/    ||subject_id|63358,63358,t,865404ea-8a9b-36c0-829f-ecc401360ae9
/
/   5. For any additional columns read in as max character length, determine actual max necessary len
/      and re-set the lenghts accordingly.
/
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ HubFiles2ReadDs  Name of the SAS dataset that contains the list of files to read that
/                  was generated from the Download360HubFiles macro
/ HubTableListDs   Name of the SAS dataset that contains one row per file with the 
/                  Table level metadata.  The Get360HubFileMetaData macro will generate
/                  this ds 
/ HubVarListDs     Name of the SAS dataset that contains one row per HUB file-column combination
/                  The Get360HubFileMetaData macro will generate this ds 
/ raw_data_path    Full path that contains all of the gzip files that were downloaded  
/ MaxCharVarLen    Default=500.  This is the initial length used to read in all character variables
/                  in the hub files.  We dont get char lenght information from CI360 so we have
/                  to choose a value here large enough to read in any/all char varaibles in any
/                  of teh 360 Hub uploaded files
/ OutLib           (default=WORK) SAS library name where the full data will be read and saved
/============================================================================================*/

%macro read_360hub_data(HubFiles2ReadDs=,
                        HubTableListDs =,
                        HubVarListDs   =,
                        raw_data_path  =,
                        MaxCharVarLen  =500,
                        outlib         =WORK) ;

  %local tableList TableNames TableNums t tableID headerfile Contact_Pref_TableID Geofence_TableID 
         TableName TableNum inputList CharVarList filelist lengthStmt DsID j DsLabel GeofenceVars2chg
         file_ext slash i obs zipfile ;

  %if (&sysscp. = WIN) %then %do;
    %let slash = %str(\);
  %end;
  %else %do;
    %let slash = %str(/);
  %end ;

  proc datasets library=WORK nolist ;
    delete _HubHeaderVars_ keyvars / memtype=data ;
  quit ;

  *** Subset the HubTableListDs and HubVarListDs to only include datasets in the HubFiles2ReadDs **;
  proc sql noprint ;
    select distinct tableID into :TableIDs2process separated by '" "' from &HubFiles2ReadDs. ;
  quit ;
  %let TableIDs2process = "&TableIDs2process." ;

  proc sort NODUPKEY data=&HubFiles2ReadDs. (keep=tableID tableNum) out=_TableNums_ ; by tableID ; run ;

  data _HubTableList_ ;
    merge &HubTableListDs. (in=inbase where=(tableID in (&TableIDs2process.)))
          _TableNums_      (in=innum)
   ;
   by tableID ;
   if not (first.tableID and last.tableID) then abort ;
   if not (inbase and innum) then abort ;
  run ;

  data _HubVarList_ ;
    set &HubVarListDs. ;
    where tableID in (&TableIDs2process.) ;
  run ;

  proc sql noprint ;
    select TableID, 
           TableName, 
           TableNum 
      into :tableList separated by " ", 
           :TableNames separated by " ", 
           :TableNums separated by " " 
    from _HubTableList_ ;
  quit ;  

  ** Process all of the file headers **;

  proc sql noprint ;
    select distinct file_ext into :file_ext from &HubFiles2ReadDs. ;
  quit ;

  %if %words(&file_ext.) > 1 %then %do ;
    %put Error: More than one file type found (&file_ext.) in &HubFiles2ReadDs. ;
    %goto FINISH ;
  %end ;
  %else %if ("%upcase(&file_ext.)" NE "CSV") AND ("%upcase(&file_ext.)" NE "GZ") %then %do ;
    %put Error: Unexpected file type found (&file_ext.) in &HubFiles2ReadDs. ;
    %goto FINISH ;
  %end ;

  %do t = 1 %to %words(&tableList.) ;
    %let tableID = %scan(&tableList.,&t.,%str( )) ;

    proc sql noprint ;
      select filepath  into :headerfile from &HubFiles2ReadDs.
      where TableID = "&tableID." AND header = 1 ;
    quit ; 

    %if "%upcase(&file_ext.)" = "CSV" %then %do ;
      filename hdr&t. %unquote(%nrbquote("%trim(&headerfile.)"));
    %end ;
    %else %if "%upcase(&file_ext.)" = "GZ" %then %do ;
      filename hdr&t. ZIP %unquote(%nrbquote("%trim(&headerfile.)")) GZIP ;
    %end ;

    data _temp_ ;
      infile hdr&t. dlm = ',' dsd LRECL=32767  ;
      length tableID %varlen(&HubFiles2ReadDs.,TableID) varname $32. ;
      retain tableID ;

      tableID = "&tableID." ;
      done = 0 ;
      iter = 1 ;

      input ;
      do until (done) ;
        varname = upcase(strip(scan(_INFILE_,iter,","))) ;
        VarOrder = iter ;
        if varname NE " " then output ;
          else done = 1 ;
        iter = iter + 1 ;
        *if iter > 20 then done = 1 ;
      end ;
      drop done iter ;
    run ;

    proc append data=_temp_ base=_HubHeaderVars_ ;
    run ;

  %end ;

  ***********************************************************;
  ** bug - Change names in header to match those from Json **;
  ***********************************************************;

  proc sql noprint ;
    select tableID into: Contact_Pref_TableID from _HubTableList_ where upcase(TableName) = "CONTACT_PREFERENCE" ;
    select tableID into: Geofence_TableID from _HubTableList_ where upcase(TableName) = "GEOFENCE_DEFINITION" ;
  quit ;

  %let GeofenceVars2chg = %cmpres(BEACON_MAJOR_NO BEACON_MINOR_NO BEACON_NM BEACON_UUID_ID CITY_NM EXTERNAL_GEOFENCE_ID 
           GEOFENCE_NM KEYWORD_TXT LATITUDE_VAL LONGITUDE_VAL EXTERNAL_APPLICATION_ID RADIUS_VAL REGION_NM STATE_NM) ;

  data _HubHeaderVars_ ;
    set _HubHeaderVars_ end=lastrec ;
    retain contactchg geofencechg 0 ;
    if tableid = "&Contact_Pref_TableID." AND varname in ("IDENTIFIER_TYPE_ID" "IDENTITY_ID" "PREFERENCE_VAL") then do ;
      contactchg = 1 ;
      if varname = "IDENTIFIER_TYPE_ID" then varname = "IDENTITY_TYPE_CD" ;
      if varname = "IDENTITY_ID" then varname = "IDENTITY_VALUE" ;
      if varname = "PREFERENCE_VAL" then varname = "PREFERENCE_VALUE" ;
    end ;
    else if tableid = "&Geofence_TableID." AND varname in (%quotelst(&GeofenceVars2chg.)) then do ;
     if varname = "BEACON_MAJOR_NO" then varname = "BEACON_MAJOR" ;
     if varname = "BEACON_MINOR_NO" then varname = "BEACON_MINOR" ;
     if varname = "BEACON_NM" then varname = "BEACON_NAME" ;
     if varname = "BEACON_UUID_ID" then varname = "BEACON_UUID" ;
     if varname = "CITY_NM" then varname = "CITY" ;
     if varname = "EXTERNAL_GEOFENCE_ID" then varname = "GEOFENCE_ID" ;
     if varname = "GEOFENCE_NM" then varname = "GEOFENCE_NAME" ;
     if varname = "KEYWORD_TXT" then varname = "KEYWORDS" ;
     if varname = "LATITUDE_VAL" then varname = "LATITUDE" ;
     if varname = "LONGITUDE_VAL" then varname = "LONGITUDE" ;
     if varname = "EXTERNAL_APPLICATION_ID" then varname = "MOBILE_APPID" ;
     if varname = "RADIUS_VAL" then varname = "RADIUS" ;
     if varname = "REGION_NM" then varname = "REGION" ;
     if varname = "STATE_NM" then varname = "STATE" ;
     geofencechg = 1 ;
    end ;
    if lastrec then do ;
      if contactchg then put "W%upcase(arning): Inconsistent variable names between metadata and header row for Contact_Preference data" ;
      if geofencechg then put "W%upcase(arning): Inconsistent variable names between metadata and header row for Geofence_definition data" ;
    end ;
    drop contactchg geofencechg ;
  run ;

  proc sort data=_HubVarList_ ; by tableID VarName ; run;
  proc sort data=_HubHeaderVars_ ; by tableID varName ; run;

  data _CombinedVars_ ;
    merge _HubVarList_    (in=inmeta drop=VarOrder)
          _HubHeaderVars_ (in=inhead)
    ;
    by tableID varName ;  
    *if not (first.varName and last.VarName) then abort ;
    if inhead and NOT inmeta then do ;
      VarType = "STRING" ;
    end ;

    injson = (inmeta) ;
    inheader = (inhead) ;

    keep tableID varName VarOrder VarLabel Vartype injson inheader ;
  run;

  proc sort data=_combinedVars_ ; by TableID VarOrder ; run ; 

  %do t = 1 %to %words(&tableList.) ;
    %let tableID = %scan(&tableList.,&t.,%str( )) ;
    %let tableName = %scan(&tableNames.,&t.,%str( )) ;
    %let tableNum = %scan(&TableNums.,&t.,%str( )) ;

    proc sql noprint ;
      select VarName into: inputList separated by " " from _combinedVars_ where tableID = "&tableID." ;
      select VarName into: CharVarList separated by " " from _combinedVars_ where tableID = "&tableID." AND upcase(VarType)="STRING";
    quit ; 
   
    filename attr TEMP ;
    ** create input and length statement from metadata **;
    data _null_ ;
      set _combinedVars_  (where=(tableID = "&tableID")) end=lastrec ;
      file attr ;

      if _N_ = 1 then put @6 "attrib" ;
      if (upcase(Vartype) = 'STRING') then 
        put @8 VarName 'FORMAT=$&MaxCharVarLen..' ;
      else if upcase(Vartype) = 'TIMESTAMP' then
        put @8 VarName 'LENGTH=8 FORMAT=DATETIME27.6 INFORMAT=ymddttm.';
      else if upcase(VarType) = 'DATE' then 
        put @8 VarName 'LENGTH=8 FORMAT=MMDDYY10. INFORMAT=yymmdd10.';
      else if upcase(Vartype) in ('SMALLINT' 'INT' 'BIGINT' 'DECIMAL' 'TINYINT' 'DOUBLE' 'BOOLEAN') then 
        put @8 VarName 'LENGTH=8.' ;
      else abort ;

      if VarLabel ne " " then 
        put @8 VarName 'label="' VarLabel +(-1) '"' ;

      if lastrec then put @6 ';' ;
    run ;

    data _files2read&t._ ;
      set &HubFiles2ReadDs. (where=(tableID = "&tableID." AND header = 0)) ;
    run ; 

    proc datasets library=WORK nolist ;
      delete &tableName. / memtype=data ;
    quit ;

    %do i = 1 %to %words(&CharVarList.) ; 
      %local max&i. ;
      %let max&i. = 0 ;
    %end ;
    %let lengthStmt = ;

    %do obs = 1 %to %nobs(_files2Read&t._) ;

      data _null_ ;
        set _files2Read&t._ (obs=&obs.) ;
        call symput("zipfile",strip(filepath)) ;
      run ;

      %if "%upcase(&file_ext.)" = "CSV" %then %do ;
        filename zip&t. "&zipfile.";
      %end ;
      %else %if "%upcase(&file_ext.)" = "GZ" %then %do ;
        filename zip&t. ZIP "&zipfile." GZIP ;
      %end ;

      data _tmp_ (compress=YES);
        infile zip&t. dlm = ',' dsd LRECL=32767 end=lastrec ;
        *** identify leading chars and read these in separately ***;
        %include attr ;

        length _var_ $32. endpos 4. lengthStmt $10000. ;
        retain _var_ endpos ;

        array chars (*) &CharVarList. ;
        array maxlen (%words(&CharVarList.)) _temporary_ (%do i=1 %to %words(&CharVarList.) ; &&max&i. %end;) ;

        if _N_ = 1 then do ;
          input @ ;  
          endpos = index(substr(_infile_,3),"|") + 3 ;
          _var_ = substr(_infile_,3,endpos-4) ;
          input @1 @;
          do _i_ = 1 to dim(maxlen) ;
            maxlen(_i_) = 0 ;
          end ;
        end ;

        input @endpos &inputList. ;

        do _i_ = 1 to dim(chars) ;
          maxlen(_i_) = max(maxlen(_i_),length(trim(chars(_i_)))) ;
        end ;

        %if (&obs. = %nobs(_files2Read&t._)) %then %do ;
          if lastrec then do ;
            lengthStmt =
              %do j = 1 %to %words(&CharVarList.) ;
              " %scan(&CharVarList.,&j.,%str( )) $" || trim(left(put(maxlen(&j.),8.))) || 
              %end ;
              "" ;
            call symput("lengthStmt",trim(left(lengthStmt))) ;
            call symput("DsID",trim(left(_var_))) ;
          end ;
        %end ;
        %else %do ;
          if lastrec then do ;
            %do j = 1 %to %words(&CharVarList.) ;
              call symput("max&j.",trim(left(put(maxlen(&j.),8.)))) ;
            %end ;
          end ;          
        %end ;

        drop _i_ endpos _var_ lengthStmt ;
      run ;

      proc append data=_tmp_ base=WORK.&tablename. (compress=YES) ;
      run ;

    %end ;

    data tmp ;
      length TableID %varlen(_HubTableList_,TableID) KeyVar $32. ;
      TableID = "&tableID." ;
      KeyVar = "&DsID." ;
    run ;

    proc append data=tmp base=KeyVars ;
    run ;

    %if %nobs(&tableName.) %then %do ;

      data &outlib..&tableName. ;
        length &lengthStmt. ;
        set &tableName. ;
      run ;

      proc compare data=&tableName. compare=&outlib..&tableName. ;
      run ;

    %end ;

    ** add label to Data set **;
    proc sql noprint ;
      select TableLongName into: DsLabel from _HubTableList_ where tableID = "&tableID." ;
    quit ;
   
    %if (%sysfunc(exist(&outlib..&tablename.)) > 0) %then %do ;
      proc datasets library=&outlib. nolist ;
        modify &tablename. (label="%trim(&DsLabel.)") ;
      quit ;
    %end ;  

  %end ;

  %FINISH:
%mend ;