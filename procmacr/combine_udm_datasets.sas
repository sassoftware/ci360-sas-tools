/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Combine_UDM_Datasets.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2019 August
/ LastModBy : Noah Powers
/ LastModDt : 11.19.2019
/ Purpose   : To append a set of discover tables for one time range to a base set of
/             discover tables.  
/             Steps:
/               1. Get list of sas datasets in Lib2Add and BaseLib from the timeperiods 
/                  datasets
/               3. Get min/max time periods for both Lib2Add and BaseLib using the files2download data
/               4. Verify that Lib2Add has newer records with reasonable overlap with BaseLib
/
/ FuncOutput: N/A
/ Usage     :
/ Notes     : 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ Lib2Add       SAS library that contains the new data to be appended to the datasets in
/               BaseLib
/ BaseLib       SAS library name where the data from Lib2Add will be appended
/ Tables2Add    (optional) If this is provided then only the tables listed in this parameter
/               and in the Lib2Add will be added to the  BaseLib
/ CompressOutput (Y/N) The default is Y and this will result in the output SAS datasets
/                that are merged will be compressed.  Any that are copied from Add to Base
/                will retain whatever compression status they had in Add./
/============================================================================================*/
%macro Combine_UDM_Datasets(Lib2Add      =,
                            BaseLib      =,
                            Tables2Add   =,
                            CompressOutput=Y
                            ) ;

  %local DS2Copy DS2Combine SortVars renameVarList NumVars2Rename _i_ ds byvars lastbyvar 
         identityDataSets wherestmt IsIdentityData inViya Lib2Add_path BaseLib_path dsopt CASOpt 
         MergeBaseLib MergeAddLib;

  %let identityDataSets = IDENTITY IDENTITY_ATTRIBUTES IDENTITY_MAP ;
  
  %if not %length(&CompressOutput.) %then %let CompressOutput = Y ;
  %let CompressOutput = %upcase(%substr(&CompressOutput.,1,1)) ;
  
  %if "%substr(&sysvlong.,1,1)" = "V" %then %let inViya = 1 ;
  %else %let inViya = 0 ;

  %if NOT %sysfunc(exist(&Lib2Add..TimeStamps)) %then %do ;
    %put Library member TimeStamps does not exist in Lib2Add (&Lib2Add.) ;
    %goto FINISH ;
  %end ;
  %if NOT %sysfunc(exist(&BaseLib..TimeStamps)) %then %do ;
    %put Library member TimeStamps does not exist in BaseLib (&BaseLib.) ;
    %goto FINISH ;
  %end ;
  %let Tables2Add = %upcase(&Tables2Add.) ;

  data _metadiffs_ ;
    delete ;
  run ;

  proc sql noprint  ;
    create table _BaseTables_ as
    select upcase(memname) as EntityName, Nobs as Obs format comma12. from sashelp.vtable
    where upcase(libname)="%upcase(&BaseLib.)" 
    order by Entityname ;

    create table _AddTables_ as
    select upcase(memname) as EntityName, Nobs as Obs format comma12. from sashelp.vtable
    where upcase(libname)="%upcase(&Lib2Add.)" 
    %if %length(&Tables2Add.) %then %do ;
      AND upcase(memname) in (%quotelst(&Tables2Add.)) 
    %end ;
    order by Entityname ;
  quit;

  %let wherestmt = ;
  %if %length(&Tables2Add.) %then %do ;
    %let wherestmt =%str(where=(upcase(EntityName) in (%quotelst(&Tables2Add.)))) ; 
  %end ;

  proc sort data=&Lib2Add..TimeStamps (keep=entityName dataRangeStart_dt dataRangeEnd_dt SortVarList) 
    out=_timeStampsAdd_ (obs=MAX &wherestmt.) ; 
    by entityName ; 
  run ;

  proc sort data=&BaseLib..TimeStamps (keep=entityName dataRangeStart_dt dataRangeEnd_dt SortVarList) 
    out=_timeStampsBase_ ; 
    by entityName ; 
  run ;

  data _timeperiods_ ;
    merge _BaseTables_     (in=inBaseDir rename=(obs=BaseObs))
          _AddTables_      (in=inAddDir rename=(obs=AddObs))
          _timeStampsBase_ (in=inBaseTS) 
          _timeStampsAdd_  (in=inAddTS rename=(dataRangeStart_dt=Add_dataRangeStart_dt 
                            dataRangeEnd_dt=Add_dataRangeEnd_dt SortVarList=Add_SortVarList))
    ;
    by entityName ; 
    if not (first.entityName and last.entityName) then abort ;
    if inBaseTS OR inAddTS ;
    if inBaseTS and NOT inBaseDir then abort ;
    if inAddTS and NOT inAddDir then abort ;
    if inAddTS and NOT inBaseTS AND inBaseDir then abort ;

    NewData = 0 ;
    CombineData = 0 ;

    if (inAddTS and not inBaseTS) then do ;
      NewData = 1 ;
      New_dataRangeStart_dt = Add_dataRangeStart_dt ;
      New_dataRangeEnd_dt = Add_dataRangeEnd_dt ;
    end ;
    else if (inBaseTS and inAddTS AND upcase(EntityName) NOT IN (%quotelst(%upcase(&identityDataSets.)))) then do ;
      if Add_dataRangeEnd_dt < dataRangeStart_dt then do ;
        if abs(Add_dataRangeEnd_dt + 1 - dataRangeStart_dt) > 10E-5 then abort ; 
        if SortVarList ne Add_SortVarList then abort ;
        CombineData = 1 ;
        New_dataRangeStart_dt = Add_dataRangeStart_dt;
        New_dataRangeEnd_dt = dataRangeEnd_dt ;
      end ;
      else if Add_dataRangeStart_dt > dataRangeEnd_dt then do ;
        if abs(dataRangeEnd_dt + 1 - Add_dataRangeStart_dt) > 10E-5 then abort ;
        if SortVarList ne Add_SortVarList then abort ;
        CombineData = 1 ;
        New_dataRangeStart_dt = dataRangeStart_dt ;
        New_dataRangeEnd_dt = Add_dataRangeEnd_dt ;
      end ;
      else do ;
        CombineData = 1 ;
        New_dataRangeStart_dt = min(dataRangeStart_dt,Add_dataRangeStart_dt) ;
        New_dataRangeEnd_dt   = max(dataRangeEnd_dt,Add_dataRangeEnd_dt) ;
      end ;
    end ;
    else if (inBaseTS and inAddTS AND upcase(EntityName) IN (%quotelst(%upcase(&identityDataSets.)))) then do ;
      CombineData = 1 ;
      if SortVarList ne Add_SortVarList then abort ;
      if Add_dataRangeStart_dt < dataRangeStart_dt then abort ;
      New_dataRangeStart_dt = Add_dataRangeStart_dt ;
      New_dataRangeEnd_dt = Add_dataRangeEnd_dt ;
    end ;
    else if (inBaseTS and NOT inAddTS) then do ;
      New_dataRangeStart_dt = dataRangeStart_dt ;
      New_dataRangeEnd_dt = dataRangeEnd_dt ;
    end ;
    else abort ;

    NobsCombined = . ;
    format NobsCombined comma15. ;
    format New_dataRangeStart_dt New_dataRangeEnd_dt datetime27.6 ;
  run ;

  %if %sysevalf(&syserr. > 0) %then %goto FINISH ;

  proc sql noprint ; 
    select lowcase(entityname) into: DS2Copy separated by " " from _timeperiods_ (where=(NewData=1)) ;
    select lowcase(entityname) into: DS2Combine separated by " " from _timeperiods_ (where=(CombineData=1)) ;
    select lowcase(SortVarList) into: SortVars separated by "|" from _timeperiods_ (where=(CombineData=1)) ;
  quit ;

  %if %words(&Ds2Combine.) %then %do ;

    data _BaseTableVars_ ;
      set sashelp.vcolumn (where=(upcase(libname)="%upcase(&BaseLib.)" AND lowcase(memname) in (%quotelst(&DS2Combine.)) AND memtype="DATA")) ;
    run ;

    data _AddTableVars_ ;
      set sashelp.vcolumn (where=(upcase(libname)="%upcase(&Lib2Add.)" AND lowcase(memname) in (%quotelst(&DS2Combine.)) AND memtype="DATA")) ;
    run ;

    proc sort data=_BaseTableVars_ ; by memname name ; run ;
    proc sort data=_AddTableVars_ ; by memname name ; run ;
    
    data _BaseTableVars_ _BaseONLYTableVars_ ;
      merge _BaseTableVars_ (in=inbase)
            _AddTableVars_  (in=inadd keep=memname name)
      ;
      by memname name ;
      if inbase ;
      if inadd then _inother_ = 1 ;
      output _BaseTableVars_ ;
      if not inadd then output _BaseONLYTableVars_ ;
    run ;
    
    %if %nobs(_BaseONLYTableVars_) %then %do ;
      title "Note: These columns are not found in the Add Library datasets" ;
      proc print data=_BaseONLYTableVars_ ;
      run ;
    %end ;
    
    data _AddTableVars_ _AddONLYTableVars_ ;
      merge _BaseTableVars_ (in=inbase keep=memname name)
            _AddTableVars_  (in=inadd)
      ;
      by memname name ;
      if inadd ;
      if inbase then _inother_ = 1 ;
      output _AddTableVars_ ;
      if not inbase then output _AddONLYTableVars_ ;
    run ;
    
    %if %nobs(_AddONLYTableVars_) %then %do ;
      title "Note: These columns are not found in the BASE Library datasets" ;
      proc print data=_AddONLYTableVars_ ;
      run ;
    %end ;

    proc compare base=_BaseTableVars_ (where=(_inother_ = 1)) compare=_AddTableVars_ (where=(_inother_ = 1)) 
      LISTALL out=_metadiffs_ OUTNOEQUAL ;
      var type length format ;
      id memname name ;
    run ;

  %end ;

  %if (%words(&DS2Copy.) > 0) %then %do ;
  
    proc copy in=&Lib2Add. out=&BaseLib. memtype=data ;
      select &DS2Copy. ;
    run ;
  
    ** update timestamps data **;
    data &baselib..TimeStamps ;
      set &baselib..TimeStamps 
          &Lib2Add..TimeStamps (in=inadd where=(lowcase(entityName) in (%quotelst(&DS2Copy.)))) 
      ;
      by entityName ;
      if not (first.EntityName and last.EntityName) then abort ;
      if inadd then last_updated_dttm = datetime() ;
      format last_updated_dttm datetime27. ;
    run ;

  %end ;

  %if %words(&Ds2Combine.) AND NOT %nobs(_metadiffs_) %then %do ;
  
    %let dsopt = ;
  
    ** If in Viya then load the identity tables needed into CAS **;
    %if (&inviya. = 1) %then %do ;
    
      %let Lib2Add_path = %sysfunc(pathname(&Lib2Add.,L)) ;
      %let BaseLib_path = %sysfunc(pathname(&BaseLib.,L)) ;
      cas _tmp_ ;  
      
      caslib Adddat_ datasource=(srctype=PATH ) path="&Lib2Add_path." ;
      libname _add_ CAS caslib="Adddat_" ;
      
      caslib Basedat_ datasource=(srctype=PATH ) path="&BaseLib_path." ;
      libname _Base_ CAS caslib="Basedat_" ;    
      
    %end ;
    %else %do ;
      %if "&CompressOutput." = "Y" %then %let dsopt = %str(compress=YES) ;
    %end ;

    %do _num_ = 1 %to %words(&DS2Combine.) ;
      %let ds = %scan(&DS2Combine.,&_num_.,%str( )) ;
      %let byvars = %scan(&SortVars.,&_num_.,%str(|)) ;
      %let lastbyvar = %scan(&byVars.,%words(&byVars.),%str( )) ;

      %let IsIdentityData = 0 ;
      %do _i_ = 1 %to %words(&identityDataSets.) ;
        %if "&ds." = "%scan(&identityDataSets.,&_i_.,%str( ))" %then 
          %let IsIdentityData = 1 ;
      %end ;

      %if (NOT &IsIdentityData.) %then %do ;
      
        %let MergeBaseLib = &BaseLib. ;
        %let MergeAddLib  = &Lib2Add. ;
            
        %if (&inviya. = 1) %then %do ;
          proc casutil incaslib="Adddat_" outcaslib="Adddat_";
            load casdata="&ds..sas7bdat" casout="&ds." replace 
            importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
          quit ;
          
          proc casutil incaslib="Basedat_" outcaslib="Basedat_";
            load casdata="&ds..sas7bdat" casout="&ds." replace 
            importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
          quit ;          
          
          %let MergeBaseLib = _Base_ ;
          %let MergeAddLib  = _Add_ ;
        %end ;
        
        data &MergeBaseLib..&ds. (&dsopt.) ;
          merge &MergeBaseLib..&ds. (in=inbase)
                &MergeAddLib..&ds.  (in=inadd)
          ;
          by &byVars. ;
          if not (first.&lastByVar. and last.&lastByVar.) then abort ;
          *if inbase and inadd then abort ;
        run ;
               
        %if (&inviya. = 1) %then %do ;
        
          %let CASOpt = ;
          %if "&CompressOutput." = "Y" %then %let CASopt = %str(compress="YES") ;
          
          proc cas;
            table.save /
              caslib="Basedat_"
              name="&ds..sas7bdat"
              table={name="&ds.", caslib="Basedat_"}
              permission="PUBLICWRITE"
              exportOptions={fileType="BASESAS" &CASopt.}
              replace=True;
          quit; 
          
          proc casutil ;
            DROPTABLE CASDATA="&ds." INCASLIB="BaseDat_" ;
          quit ;
        %end ;
        
      %end ;
      %else %do ;
        proc copy in=&Lib2Add. out=&BaseLib. memtype=data ;
          select &ds. ;
        run ;
      %end ;
      
      data _timeperiods_  ;
        set _timeperiods_  ;
        if upcase(entityName) = upcase("&ds.") then do ;
          NobsCombined = %nobs(&BaseLib..&ds.) ;
        end ;
      run ;

    %end ;

    %let renameVarList = %removewords(%upcase(%varlist(&Lib2Add..TimeStamps)),ENTITYNAME DATARANGESTART_DT DATARANGEEND_DT) ;
    %let NumVars2Rename = %words(&renameVarList.) ;

    ** update timestamps data **;
    data &baselib..TimeStamps ;
      merge &baselib..TimeStamps (in=inbase)
            _timeperiods_        (in=innewtp keep=entityName New_dataRangeStart_dt New_dataRangeEnd_dt NobsCombined )
            &Lib2Add..TimeStamps (in=inadd drop=DATARANGESTART_DT DATARANGEEND_DT
                                     where=( lowcase(entityName) in (%quotelst(&DS2Combine.)) )
                                     rename=(%do _i_ = 1 %to &numVars2Rename.; 
                                       %scan(&renameVarList.,&_i_.,%str( ))=%scan(&renameVarList.,&_i_.,%str( ))_ %end;)) 
      ;
      by entityName ;
      if not (first.EntityName and last.EntityName) then abort ;
      if inbase ;
      if inadd AND not innewtp then abort ;
      if inbase and inadd then do ;
        schemaVersion_orig = schemaVersion ;
        %do _i_ = 1 %to &numVars2Rename.; 
          %scan(&renameVarList.,&_i_.,%str( )) = %scan(&renameVarList.,&_i_.,%str( ))_ ;
        %end;
        dataRangeStart_dt =New_dataRangeStart_dt ;
        dataRangeEnd_dt   =New_dataRangeEnd_dt ;
        last_updated_dttm = datetime() ;
      end ;
      if NobsCombined = . then NobsCombined = NobsFinal ;
      drop New_dataRangeStart_dt New_dataRangeEnd_dt ;
      drop %do _i_ = 1 %to &numVars2Rename.; %scan(&renameVarList.,&_i_.,%str( ))_ %end;;
      format last_updated_dttm datetime27. ;
    run ;

  %end ;
  
  %if (&inviya. = 1) %then %do ;
    cas _tmp_ terminate ;
  %end ;
    
  %FINISH: 

%mend ;
