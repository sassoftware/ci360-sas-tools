/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Remove_UDM_Data_Dups.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2020 March
/ LastModBy : Noah Powers
/ LastModDt : 04.12.2021
/ Purpose   : Remove duplicate records from disocver/engage data tables.  If this code is
/             run in a Viya environment, then the input tables will automatically be loaded
/             into a CASlib in-memory table (if not already in such library) so that the 
/             sorting is not needed and the processing will complete faster than sorting the 
/             non-CASlib input tables.
/
/             If the input data is in a SAS9.4 environment and the data needs to be sorted,
/             then a custom built in-memory sorting algorithm is used that is essentially
/             identical to the TAGSORT option on proc sort except that the SASFILE statement
/             is used to load data into memory for sorting the TAGSORT way.
/ FuncOutput: N/A
/ Usage     :
/ Notes     : Detect existing dataset options and automaticaly enforce those on the 
/             sorted output data
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ inlib              Name of the SAS library that contains the discover/engage sas datasets 
/                    or other valid sas library data source such as CASlib (not gzip files) 
/                    to have the duplicates removed.
/ SortInputData      Default=Y (Y/N) Does the input data sets need to be sorted (in a temporary
/                    copy of the data).  The original data stays as is.  The default is 
/                    set to Y, but if this code is run in a Viya environment then the input data
/                    will automatically be loaded into an in-memory CASlib for processing so that
/                    sorting is not needed.  Also if this paramter is set to Y but the input
/                    data is already in a CAS library then no sorting will be performed.
/ DupLib             Name of the SAS library where the removed duplicate records are to be written.
/                    The default value for this is the same as inlib or if the user explicitly 
/                    supplies a blank value for this parameter
/ TimeStampDs        (&inlib..timestamps) The timestamps dataset created by read_udm_data.sas
/                    that contains the list of datasets to read
/ MakeIdentityIDIndx Y/N (default=N) Make an index on the output dataset to create an index on the 
/                    identity_id variable (if present in the data)
/ SortVarXL          Is full path and name of Excel file that is exepcted to contain a sheet
/                    named sortVars with the 3 columns: EntityName  SortVarList TieBreakVar
/ SortVarsXLSheet    The name of the sheet in SORTVARXL Excel spreadsheet to use for the 
/                    sort variables for each table.  The Deafalt value is SortVars.
/ CompressOutput     (Y/N) The default is Y and this will result in the output SAS datasets
/                    will be compressed.  
/ ProcSortOpts       (optional) If supplied, this string is added to the proc sort options when
/                    sorting the source datasets for duplicate removal.  The default is sortsize=25G
/ PctDupsErr         This is a percentage where 1% is represented by 0.01.  If the number of duplicate
/                    records removed from the original data is greater than this percent, an error
/                    message is written to the log to signal that further investigation may be 
/                    warranted.
/============================================================================================*/
%macro Remove_UDM_Data_Dups(inlib             =,
                            SortInputData     =Y,
                            duplib            =,
                            TimeStampsDs      =&inlib..timestamps,
                            MakeIdentityIDIndx=N,
                            SortVarsXL        =,
                            SortVarsXLSheet   =SortVars,
                            CompressOutput    =Y,
                            ProcSortOpts      =%str(sortsize=25G),
                            PctDupsErr        =%str(0.01)
                            ) ;

  %local tableList t table SortVarLists TieBreakVars outlib inViya libEngine CaslibFound Load2CAS 
         NumCASTables NumCASFiles inCASlib ext ftype outdsopts SourceNameList MaxLen 
         FileExtList MemoryFlagList ;
  
  %if NOT %length(&duplib.) %then %let duplib = &inlib. ;
  %if NOT %length(&MakeIdentityIDIndx.) %then %let MakeIdentityIDIndx = N ;
  %if NOT %length(&TimeStampsDs.) %then %let TimeStampsDs = &inds..timestamps ;
  %let MakeIdentityIDIndx = %upcase(%substr(&MakeIdentityIDIndx.,1,1)) ;
  %let SortInputData = %upcase(%substr(&SortInputData.,1,1)) ;
  
  %if not %length(&CompressOutput.) %then %let CompressOutput = Y ;
  %let CompressOutput = %upcase(%substr(&CompressOutput.,1,1)) ;
  
  %if "%substr(&sysvlong.,1,1)" = "V" %then %let inViya = 1 ;
  %else %let inViya = 0 ;
  
  proc sql noprint ;
    create table _libMeta_ as select * from sashelp.vlibnam where upcase(libname) = "%upcase(&inlib.)" ;
    select upcase(sysvalue) into: inCASlib from _libMeta_ where upcase(sysname) = "CASLIB" ;
  quit ;
  %let inCASlib = %trim(&inCASlib.) ;
  
  libname _xl_ XLSX "&SortVarsXL." ;  

  data _SortVars_ ;
    length entityName $32. ;
    set _xl_.&SortVarsXLSheet. (keep=EntityName SortVarList TieBreakVar) ;
    EntityName = strip(upcase(EntityName)) ;
    SortVarList = strip(upcase(SortVarList)) ; 
    TieBreakVar = strip(upcase(TieBreakVar)) ;
    if entityName > "" ;
  run ;
 
  proc sort data=_SortVars_ ; by EntityName ; run ;
  proc sort data=&TimeStampsDs. ; by EntityName ; run ;

  data TablesInLibOnly (keep=entityName dataRangeStart_dt dataRangeEnd_dt)
       TablesinXLOnly (keep=EntityName SortVarList TieBreakVar) 
       _Tables2Process_ ;
    merge _SortVars_     (in=inXL)
          &TimeStampsDs. (in=inTS keep=entityName dataRangeStart_dt dataRangeEnd_dt)
    ;
    by entityName ;
    length NobsOrig NobsFinal DupObs PctObsRemoved UpdateFlag 8. ;
    
    if not (first.entityName and last.entityName) then abort ;
    if (inXL AND not inTS) then output TablesinXLOnly ;
    else if (inTS and not inXL) then output TablesInLibOnly ;
    else do ;
      if strip(SortVarList) <= " " then abort ;
      output _Tables2Process_ ;
    end ;
    format dataRangeStart_dt dataRangeEnd_dt datetime19.  ;
  run ;

  %if %sysevalf(&syserr. > 0) %then %goto FINISH ;

  title "Note: These tables in Excel sort file will be ignored because they are NOT found in &inlib." ;
  proc print data=TablesinXLOnly ;
  run ;

  %if %nobs(TablesinLibOnly) %then %do ;
    title "%upcase(Error): These Tables in (&inlib.) were not inlcuded in Excel input file" ;
    proc print data=TablesInLibOnly ;
    run ;

    %goto FINISH ;
  %end ;

  %if (&inViya. = 0) OR ((&inViya. = 1) AND (&libEngine. = V9)) %then %do ;
    %let Load2CAS = 0 ;
  %end ;
  %else %if (&inViya. = 1) %then %do ;
  
    proc cas;
      table.caslibinfo result=res1 status=stat / srcType='PATH';
      verbose=TRUE;
      run;
    
      table1 = findtable(res1);
      saveresult table1 dataout=work.CASLIBS;
      run;
    quit;
    
    %if (&syserr. = 4) %then %do ;
      %put Error: It appears that there is not an active CAS session or caslib ;
      %goto FINISH ;
    %end ;
    
    data _null_ ;
      set CASlibs end=lastrec ;
      retain CaslibFound 0 ;
      if upcase(name) = "&inCASlib." then CaslibFound = 1 ;
      if lastrec then call symput("CaslibFound",put(CaslibFound,8.)) ;
    run ;
    
    %if (&CaslibFound. = 1) %then %do ;
      %let Load2CAS = 1 ;
      
      proc datasets library=WORK nolist ;
        delete _Tablemeta_ _Filemeta_ / memtype=data ;
      quit ;
      
      ods output TableInfo=_Tablemeta_ Fileinfo=_Filemeta_ ;
      
      proc casutil incaslib="&inlib." ;
        list tables;
        list files;
      quit ;
      
      %let NumCASTables = 0 ;
      %let NumCASFiles = 0 ;

      %if %sysfunc(exist(_Tablemeta_)) %then %let NumCASTables = %nobs(_Tablemeta_) ;
      %if %sysfunc(exist(_Filemeta_)) %then %do ;
        data _Filemeta_ ;
          set _Filemeta_ ;
          if upcase(scan(Name,2,".")) in ("SAS7BDAT" "SASHDAT" "CSV") ;
        run ;
        %let NumCASFiles  = %nobs(_Filemeta_) ;
      %end ;

      %put NumCASTables=&NumCASTables. ;
      %put NumCASFiles=&NumCASFiles. ;
    
      %if %eval(&NumCASTables. > 0) AND %eval(&NumCASFiles. > 0) %then %do ;
      
        proc sort data=_filemeta_ (rename=(name=sourceName permission=FilePermission owner=FileOwner group=Filegroup
                                           size=FileSize encryption=FileEncryption modtime=FileModTime)) ;
          by SourceName ; 
        run ;
        
        proc sort data=_tableMeta_ (where=(sourceName > " ")) out=_tableMeta2_ ; by sourceName ; run ;
        
        %let MaxLen = %sysfunc(max(%varlen(_tableMeta2_,SourceName,IncludeDollarSign=N),%varlen(_FileMeta_,SourceName,IncludeDollarSign=N))) ;
        data _tableANDfile_ 
             _fileOnly0_ (keep=sourceName FilePermission FileOwner Filegroup FileSize FileEncryption FileModTime) ;
          length SourceName $&maxlen..  ;
          merge _tableMeta2_ (in=intable)
                _fileMeta_  (in=infile)
          ;
          by SourceName ;
          if not (first.SourceName and last.SourceName) then abort ;
          name = upcase(name) ;
          if intable then output _tableANDfile_ ;
          if infile and NOT intable then output _fileOnly0_ ;
        run ;
        
        data _fileOnly_ ;
          set _fileOnly0_ ;
          name = upcase(scan(SourceName,1,".")) ;
          FileExt = scan(sourceName,2,".") ;
        run ;

        data _tableMeta_ ;
          set _tableMeta_ ;
          name = upcase(name) ;
        run ;
          
        proc sort data=_fileOnly_ ; by name ; run ;
        proc sort data=_tableMeta_ (where=(sourceName <= " ")) out=_MemoryOnly_ ; by Name ; run ;
        proc sort data=_tableANDfile_ ; by name ; run ;
        
      %end ;
      %else %if %eval(&NumCASTables. > 0) %then %do ;
      
        proc sort data=_Tablemeta_ out=_MemoryOnly_ ; by name ; run ;
      
      %end ;
      %else %if %eval(&NumCASFiles. > 0) %then %do ;
      
        data _fileOnly_ ;
          set _Filemeta_ ;
          name = upcase(scan(SourceName,1,".")) ;
          FileExt = scan(sourceName,2,".") ;
        run ;
      
        proc sort data=_fileOnly_; by name ; run ;
           
      %end ;
      %else %do ;
        %put Error: No tables and No files found in caslib (&inlib.) ;
        %goto FINISH ;
      %end ;
        
      data _Tables2Process_ (rename=(name=entityName)) ;
        merge _Tables2Process_ (in=inlist rename=(entityName=Name)) 
              %if %sysfunc(exist(_tableANDfile_)) %then %do ;
                %let keepit = ;
                %if NOT %sysfunc(exist(_fileOnly_)) %then %let keepit = sourceName ;
                _tableANDfile_   (in=inboth keep=name &keepit.)
              %end ;
              %if %sysfunc(exist(_fileOnly_)) %then %do ;
                _fileOnly_       (in=infonly keep=name sourceName FileExt)
              %end ;
              %if %sysfunc(exist(_MemoryOnly_)) %then %do ;
                _MemoryOnly_     (in=inmonly keep=name)
              %end ;
        ;
        by name ;
        if not (first.name and last.name) then abort ;
        if inlist ;
        %if NOT %sysfunc(exist(_fileOnly_)) %then %do ;
          infonly = 0 ;
        %end ;
        %if NOT %sysfunc(exist(_MemoryOnly_)) %then %do ;
          inmonly = 0 ;
        %end ;
        %if NOT %sysfunc(exist(_tableANDfile_)) %then %do ;
          inboth = 0 ;
        %end ;
        if inboth AND infonly then abort ;        
        if inboth OR inmonly then InMemory = 1 ;
        else inMemory = 0 ;
        if infonly then InFile = 1 ;
          else InFile = 0 ;
      run ;

      proc sql noprint ;
        select sourceName into: SourceNameList separated by "|" from _Tables2Process_ ;
        select FileExt into: FileExtList separated by "|" from _Tables2Process_ ;        
        select inMemory into: MemoryFlagList separated by " " from _Tables2Process_ ;
      quit ;  
       
    %end ;
    %else %do ;
      %put Error: User specified inlib is not CASlib and not SAS dataset library ;
      %goto FINISH ;
    %end ;
  %end ;
  %else %do ;
    %put Error: Unexpected type of inlib=&inlib. ;
    %goto FINISH ;
  %end ;

  proc sql noprint ;
    select entityName into: tableList separated by " " from _Tables2Process_ ;
    select SortVarList into: SortVarLists separated by "|" from _Tables2Process_ ;
    select TieBreakVar into: TieBreakVars separated by "|" from _Tables2Process_ ;
  quit ;  

  %do t = 1 %to %words(&tableList.) ;
    %let table = %scan(&tableList.,&t.,%str( ));
    %let SortVars = %upcase(%scan(&SortVarLists.,&t.,%str(|))) ;
    %let LastSortVar = %scan(&SortVars.,%words(&SortVars.),%str( )) ;
    %let TieBreakVar = %upcase(%scan(&TieBreakVars.,&t.,%str(|))) ;
    
    %if ("&Load2CAS." = "0") AND ("&SortInputData." = "Y") %then %do ;
      %let outlib = &inlib. ;
    
      %Tagsort_InMem(inds       =&inlib..&table.,
                     outds      =&inlib..&table.,
                     outdsOpts  =&outdsOpts.,
                     sortbyVars =&SortVars. &TieBreakVar.,
                     sortOpts   =&ProcSortOpts.) ;

    %end ;
    %else %if ("&Load2CAS." = "1") AND (%scan(&MemoryFlagList.,&t.,%str( )) = 0) %then %do ;

      ** If only on disk then load into memory **;       
      proc casutil incaslib="&incaslib." ;
        load casdata="%scan(&SourceNameList.,&t.,%str(|))" casout="&table." outcaslib="&incaslib.";
      quit ;

      %let outlib=&inlib. ;
    %end ;
    
    %if ("&CompressOutput." = "Y") AND (&inViya. = 0) %then %let outdsopts = %str(compress=YES) ;
    %else %let outdsopts = %str() ;
  
    data &outlib..table&t._dups  (&outdsOpts.) 
         &outlib..table&t._dedup (&outdsOpts.) ;
      set &inlib..&table. ;
      by &SortVars. ;
      if first.&lastSortVar. then output &outlib..table&t._dedup ;
      else output &outlib..table&t._dups ;
    run ;

    %if %sysevalf(&syserr. = 0) %then %do ;
   
      data _Tables2Process_ ;
        set _Tables2Process_ ;
        if upcase(entityName) = upcase("&table.") then do ;
          NobsOrig = %nobs(&inlib..&table.) ;
          NobsFinal = %nobs(&outlib..table&t._dedup) ;
          %if (%sysfunc(exist(&outlib..table&t._dups)) > 0) %then %do ; 
            DupObs    = %nobs(&outlib..table&t._dups) ;
          %end ;
          %else %do ;
            DupObs    = 0 ;
          %end ;
          PctObsRemoved = DupObs/NobsOrig ;
          updateFlag    = 1 ;
        end ;
        format NobsOrig NobsFinal DupObs comma15. PctObsRemoved percent8.2 ;
      run ;

      %if (%nobs(&outlib..table&t._dups) > 0) %then %do ;      
        
        ** if CAS lib and there is underlying file then update the file **;
        %if ("&Load2CAS." = "1" AND %length(%scan(&SourceNameList.,&t.,%str(|)))>0) %then %do ;   
        
          %let ext = %scan(&FileExtList.,&t.,%str(|)) ;
          %if "%upcase(&ext.)" = "SAS7BDAT" %then %do ;
            %let ftype = BASESAS ;
          %end ;
          %else %if "%upcase(&ext.)" = "CSV" %then %do ;
            %let ftype = CSV ;
          %end ;
          %else %do ;
            %let ftype = AUTO ;
          %end ;
          
          ** Overwrite the source file with de-duped data **;
          %if ("&CompressOutput." = "Y") %then %let outdsopts = %str(compress="YES") ;
            %else %let outdsopts = %str() ;
  
          proc cas;
            table.save /
              caslib="&incaslib."
              table={name="table&t._dedup", caslib="&incaslib."}
              name="%scan(&SourceNameList.,&t.,%str(|))"     
              permission="PUBLICWRITE"
              exportOptions={fileType="&ftype." &outdsOpts.}
              replace=True;
              
            table.fileInfo /                                            
              path="%scan(&SourceNameList.,&t.,%str(|))";
          quit;

          ** Save duplicate records in same kind of file **;  
          proc cas;
            table.save /
              caslib="&duplib."
              table={name="table&t._dups", caslib="&incaslib."}
              name="%scan(&SourceNameList.,&t.,%str(|))"     
              permission="PUBLICWRITE"
              exportOptions={fileType="&ftype." &outdsOpts.}
              replace=True;
              
            table.fileInfo /                                            
              path="%scan(&SourceNameList.,&t.,%str(|))";
          quit;

                           
        %end ;
        %else %do ;
          ** Save de-duped data over the original file **;
          proc datasets library=&inlib. nolist ;
            delete &table. ;
            change table&t._dedup=&table. ;
          quit ;

          ** Save duplicate records **;
          proc copy MOVE in=&inlib. out=&duplib. memtype=data ;
            select table&t._dups ; 
          run ;
          
          proc datasets library=&duplib. nolist ;
            change table&t._dups=%scan(&SourceNameList.,&t.,%str(|));
          quit ;
          
        %end ;
      %end ;
      %else %do ;
        ** delete the _dups and _dedup datasets/memory tables **;
        proc datasets library=&inlib. nolist ;
          delete table&t._dedup table&t._dups / memtype=data ;
        quit;
      %end ;
      
      %if ("&MakeIdentityIDIndx." = "Y") %then %do ;
        ** check if index on identity_id exists - if not create one **;
        data _indx ;
          set sashelp.vindex (where=(upcase(libname)="%upcase(&inlib.)" AND upcase(memname)="%upcase(&table.)" AND 
                                     upcase(name)="IDENTITY_ID" AND upcase(idxusage)="SIMPLE")) ;
        run ;
  
        %if %length(%match(%upcase(%varlist(&inlib..&table.)),IDENTITY_ID)) AND (%nobs(_indx) <= 0) %then %do ;
          proc datasets library=&inlib. nolist ;
            modify &table. ;
            index create identity_id ;
          quit ;
        %end ;
      %end ;
      ** if there was NO in-memory table to start with - then delete it now **;
      %if ("&Load2CAS." = "1") AND (%scan(&MemoryFlagList.,&t.,%str( )) = 0) %then %do ;
        proc datasets library=&inlib. nolist ;
          delete &table. ;
        quit ;
      %end ;
    
    %end ;
  %end ;

  proc sql noprint ;
    select max(PctObsRemoved) into: MaxPctObsRemoved from _Tables2Process_ ;
  quit ;  

  %if %sysevalf(&MaxPctObsRemoved. > &PctDupsErr.) %then %do ;
    %put E%upcase(rror:) One or more tables had > &PctDupsErr. percent obs removed as duplicates ;
  %end ;

   data &TimeStampsDs. ;
    merge _Tables2Process_   (in=innew keep=EntityName UpdateFlag NobsOrig NobsFinal DupObs PctObsRemoved SortVarList
                                rename=(NobsOrig=NobsOrig0 NobsFinal=NobsFinal0 DupObs=DupObs0 
                                        PctObsRemoved=ctObsRemoved0 SortVarList=SortVarList0))
          &TimeStampsDs.     (in=inmain)
    ;
    by entityName ;
    if not (first.entityName and last.entityName) then abort ;
    if inmain ;
    if innew and updateFlag = 1 then do ;
      NobsOrig=NobsOrig0 ;
      NobsFinal=NobsFinal0 ;
      DupObs=DupObs0 ;
      PctObsRemoved=ctObsRemoved0 ;
      SortVarList=SortVarList0 ;
      DupsRemoved_dt = datetime() ;
    end ;
    format NobsOrig NobsFinal DupObs comma15. PctObsRemoved percent8.2 DupsRemoved_dt datetime19. ;
    format dataRangeStart_dt dataRangeEnd_dt datetime19.  ;
    drop NobsOrig0 NobsFinal0 DupObs0 ctObsRemoved0 UpdateFlag SortVarList0 ;
  run ;

  %FINISH:
%mend ;