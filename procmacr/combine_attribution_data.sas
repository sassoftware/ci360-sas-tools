/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : combine_Attribution_Data.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2020 October
/ LastModBy : Noah Powers
/ LastModDt : 10.28.2020
/ Purpose   : Combine internal and external attribution input data into a single dataset and 
/             create the AWS attribution process expected control A delimited data.
/
/             Validate that the expected variables are included in both the internal and
/             external data and that they have the correct lengths and types.  If the external 
/             data has char lengths longer than expected, the actual values are checked to 
/             make sure no values are longer than AWS process expects.
/
/             If list of variables that uniquely define rows in the internal and/or external
/             data is provided, then duplicates are removed from the respective datasets
/
/             - Check for missing values for identity_id
/             - interaction_type must be one of the following three values: Origination, Conversion, and Task
/             - interaction_id is the dataview ID when interaction_type = Conversion and it is the session ID
/               when the interaction_type is origination.  A report for the conversions is provided to user
/             - Report on overlap between: identity_id, interaction and interaction_subtype values between
/               internal and external data
/             - Report on overlap of time periods and distribution of data across time 
/             - Report on when date time values only have date part
/
/ FuncOutput: N/A
/ Usage     :
/ Notes     : 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ internalAttrDs   The SAS dataset that contains the CI360 generated attribution_ABT
/ intUniqueVars    (optional) The list of variables in the INTERNALATTRDS that are expected
/                  to uniquely determine each row.  If this list is provided, then 
/                  duplicates are removed and reported on.
/ externalAttrDs   The SAS datset with external intertactions not already captured in the 
/                  CI360 attribution data.
/ extUniqueVars    (optional) The list of variables in the EXTERNALATTRDS that are expected
/                  to uniquely determine each row.  If this list is provided, then 
/                  duplicates are removed and reported on.
/ dlm              The delimiter to use in the ASCII text file exported by this code.  The
/                  default value is cotnrol A or %str('01'x)
/ OutfileName      The full file path and file name that will contain the combined 
/                  internal and external data.  This file will be compressed as well.
/============================================================================================*/
%macro combine_Attribution_Data(internalAttrDs =,
                                intUniqueVars  =,
                                externalAttrDs =,
                                extUniqueVars  =,
                                dlm            =%str('01'x),
                                OutfileName    =) ;

  %local FinalExtAttrDs FinalIntAttrDs MissingVars typeMismatchVars Var2Long exp_lengths 
         Vars2Short Varlens i var explen ExitFlag LastSortVar ;

  %let FinalExtAttrDs = &externalAttrDs. ;
  %let FinalIntAttrDs = &internalAttrDs. ;

  %if NOT %length(&internalAttrDs.) and NOT %length(&externalAttrDs.) %then %do ;
    %put %str(Error: User must provide values for at least one of the following: internalAttrDs and/or externalAttrDs ) ;
    %goto FINISH ;
  %end ;

  title " " ;

  *------------------------------------------------------------------------------------*;
  *                  Create Expected Attribution MetaData dataset                      *;
  *------------------------------------------------------------------------------------*;

  data attrMetaData ;
    length name $32. exp_type exp_length 8.;
    name = "conversion_value" ;
    exp_type = 1 ;
    exp_length = 8 ;
    output ;
    
    name = "creative_id" ;
    exp_type = 2 ;
    exp_length = 36;
    output ;

    name = "identity_id" ;
    exp_type = 2 ;
    exp_length = 36;
    output ;

    name = "interaction" ;
    exp_type = 2 ;
    exp_length = 260;
    output ;

    name = "interaction_cost" ;
    exp_type = 1 ;
    exp_length = 8;
    output ;

    name = "interaction_dttm" ;
    exp_type = 1 ;
    exp_length = 8;
    output ;

    name = "interaction_id" ;
    exp_type = 2 ;
    exp_length = 36;
    output ;

    name = "interaction_subtype" ;
    exp_type = 2 ;
    exp_length = 100;
    output ;

    name = "interaction_type" ;
    exp_type = 2 ;
    exp_length = 15;
    output ;

    name = "load_id" ;
    exp_type = 2 ;
    exp_length = 36;
    output ;

    name = "task_id" ;
    exp_type = 2 ;
    exp_length = 36;
    output ;
  run ;

  proc sort data=attrMetaData ; by name ; run ;

  *------------------------------------------------------------------------------------*;
  *                       Validate External Attribution metaData                       *;
  *------------------------------------------------------------------------------------*;

  %if %length(&externalAttrDs.) %then %do ;
    
    proc contents data=&externalAttrDs. noprint out=extConts (keep=name type length) ;
    run ;

    data extConts ;
      set extConts ;
      name = lowcase(name) ;
    run ;

    proc sort data=extConts ; by name ; run ;

    data chkAttr addlExtVars ;
      merge extConts     (in=inext)
            attrMetaData (in=inexp)
      ;
      by name ;
      if not (first.name and last.name) then abort ;
      if inExt and NOT inexp then output AddlExtVars ;
      else if inExp and NOT inExt AND name not in ("creative_id" "interaction_cost" "load_id" "task_id") then missingVar = 1 ;
      else if inexp and inExt then do ;
        if type ne exp_type then typeMismatch = 1 ;
        else if type = 2 AND length > exp_Length then Var2Long = 1 ;
        else if type = 2 AND length < exp_Length then Var2Short = 1 ;
      end ;
      output chkAttr ;
    run ;

    %if %nobs(addlExtVars) %then %do ;
      %put %str(Note: Unexpected variables found in &externalAttrDs. that will be ignored.  See WORK.addlExtVars for details) ;
    %end ;

    proc sql noprint ;
      select name into: MissingVars separated by " " from chkAttr (where=(missingVar=1)) ;
      select name into: typeMismatchVars separated by " " from chkAttr (where=(typeMismatch=1)) ;
      select name into: Vars2Long separated by " " from chkAttr (where=(Var2long=1)) ;
      select exp_length into: Exp_Lengths separated by " " from chkAttr (where=(Var2long=1)) ;
      select name into: Vars2Short separated by " " from chkAttr (where=(Var2Short=1)) ;
      select exp_length into: Varlens separated by " " from chkAttr (where=(Var2Short=1)) ;
    quit ;

    %let MissingVars = %trim(&MissingVars.) ;
    %let typeMismatchVars = %trim(&typeMismatchVars.) ;
    %let Vars2Long = %trim(&Vars2Long.) ;
    %let exp_lengths = %trim(&exp_lengths.) ;
    %let Vars2Short = %trim(&Vars2Short.) ;
    %let VarLens = %trim(&VarLens.) ;

    %if %length(&Vars2Long.) OR %length(&Vars2Short.) %then %do ;
      data ExternalCharVars2long 
           ExternalAttr0 (drop=Var2LongFlag _Longvar1-_Longvar%words(&Vars2Long.) _shortVar1-_ShortVar%words(&Vars2short.)) ;
        set &externalAttrDs. (rename=(%do i = 1 %to %words(&Vars2Long.) ; %scan(&Vars2Long.,&i.,%str( ))=_Longvar&i. %end ;
                                      %do i = 1 %to %words(&Vars2Short.) ; %scan(&Vars2Short.,&i.,%str( ))=_Shortvar&i. %end ;)) ;
        %do i = 1 %to %words(&Vars2Short.) ;
          %let var = %scan(&Vars2Short.,&i.,%str( )) ;
          %let expLen = %scan(&VarLens.,&i.,%str( )) ;
          length &var. $&expLen.. ;
          &var. = trim(left(_Shortvar&i.)) ;
        %end ;
        %do i = 1 %to %words(&Vars2Long.) ;
          %let var = %scan(&Vars2Long.,&i.,%str( )) ;
          %let expLen = %scan(&Exp_Lengths.,&i.,%str( )) ;
          length &var. $&expLen.. ;
          &var. = trim(left(_Longvar&i.)) ;
          if length(trim(&var.)) > &expLen. then Var2LongFlag = 1 ;
        %end ;
        output ExternalAttr0 ;
        if Var2LongFlag = 1 then output ExternalCharVars2long ;
        %let FinalExtAttrDs = ExternalAttr0 ;
      run ;

      %if %nobs(ExternalCharVars2long) %then %do ;
        %put %str(Error: Some char var(s) values in &externalAttrDs. have too long lenghts.  The records are in WORK.ExternalCharVars2long) ;
        %let ExitFlag = 1 ;
      %end ;
    %end ;

    %if %length(&MissingVars.) %then %do ;
      %put %str(Error: the following required variables: &MissingVars. were not found in &externalAttrDs.) ;
      %let ExitFlag = 1 ;
    %end ;
    %if %length(&typeMismatchVars.) %then %do ;
      %put %str(Error: the following variables: &typeMismatchVars. have the wrong type in &externalAttrDs.) ;
      %let ExitFlag = 1 ;
    %end ;
    
    %if (&ExitFlag. = 1) %then %goto FINISH ;
    
  %end ;

  *------------------------------------------------------------------------------------*;
  *                       Validate Internal Attribution Data metadata                  *;
  *------------------------------------------------------------------------------------*;

  %if %length(&internalAttrDs.) %then %do ;
    
    proc contents data=&internalAttrDs. noprint out=intConts (keep=name type length) ;
    run ;

    data intConts ;
      set intConts ;
      name = lowcase(name) ;
    run ;

    proc sort data=intConts ; by name ; run ;

    title " " ;
    proc compare base=attrMetaData compare=intConts out=IntMetadiffs outnoequal ;
    run ;

    %if %nobs(IntMetadiffs) %then %do ;
      %put %str(Error: Internal attribution data metadata not as expected see WORK.IntMetadiffs for details) ;
      %goto FINISH ;
    %end ;
    
  %end ;

  *------------------------------------------------------------------------------------*;
  *                Identify and remove duplicate records if requested                  *;
  *------------------------------------------------------------------------------------*;

  %if %length(&FinalIntAttrDs.) AND %length(&intUniqueVars.) %then %do ;

    %let LastSortVar = %scan(&intUniqueVars.,%words(&intUniqueVars.),%str( )) ;
    proc sort data=&FinalIntAttrDs. ; by &intUniqueVars. ; run ;

    data _intAttrDs_ removed_int ;
      set &FinalIntAttrDs. ; 
      by &intUniqueVars. ; 
      if first.&LastSortVar. then output _intAttrDs_ ;
      else output removed_int ;
    run ;

    %let FinalIntAttrDs = WORK._intAttrDs_ ;
    data _null_ ;
      file print ;
      put ;
      put "Note: %nobs(removed_int) duplicate observations or %sysevalf( %nobs(removed_int) / %nobs(&internalAttrDs.) ) percent removed from Internal Attribution data" ;
      put ;
    run ;

  %end ;

  %if %length(&FinalExtAttrDs.) AND %length(&extUniqueVars.) %then %do ;

    %let LastSortVar = %scan(&extUniqueVars.,%words(&extUniqueVars.),%str( )) ;
    proc sort data=&FinalExtAttrDs. ; by &extUniqueVars. ; run ;

    data _extAttrDs_ removed_ext ;
      set &FinalExtAttrDs. ; 
      by &extUniqueVars. ; 
      if first.&LastSortVar. then output _extAttrDs_ ;
      else output removed_ext ;
    run ;

    %let FinalExtAttrDs = WORK._extAttrDs_ ;
    data _null_ ;
      file print ;
      put ;
      put "Note: %nobs(removed_ext) duplicate observations or %sysevalf(%nobs(removed_ext)/%nobs(&externalAttrDs.)) percent removed from External Attribution data" ;
      put ;
    run ;

  %end ;

  *------------------------------------------------------------------------------------*;
  *                Create Combined Data File and run checks on the data                *;
  *------------------------------------------------------------------------------------*;

  data combined Identity_missing ;
    set %if %length(&FinalIntAttrDs.) %then %do ;
          &FinalIntAttrDs. (in=internal)
        %end ;
        %if %length(&FinalExtAttrDs.) %then %do ;
          &FinalExtAttrDs. (in=external) end=lastrec
        %end ;
        ;
    %if NOT %length(&FinalIntAttrDs.) %then %do ;
      inInt = 0 ;
      inExt = (external) ;
    %end ;
    %if NOT %length(&FinalExtAttrDs.) %then %do ;
      inInt = (internal) ;
      inExt = 0 ;
    %end ;
    
    date = datepart(interaction_dttm) ;
    time = timepart(interaction_dttm) ;
    output combined ;
    if identity_id in ("" ".") then output Identity_missing ;
    format date mmddyy8. time time8. ;
  run ;

  %if %length(&FinalExtAttrDs.) %then %do ;
    data _null_ ;
      file print ;
      put ;
      put "Note: %nobs(combined (where=(inExt = 1 and time = 0))) of %nobs(combined (where=(inExt = 1))) observations in External Data have no time part" ;
      put ;
    run ;
  %end ;

  %if %nobs(Identity_missing) %then %do ;
    title "Error: %nobs(Identity_missing) records found with missing values for Identity_id" ;
    proc print data=Identity_missing (obs=10);
    run ;
  %end ;

  %if %length(&FinalExtAttrDs.) > 0 and %length(&FinalIntAttrDs.) > 0 %then %do ;
    proc sort NODUPKEY data=&FinalExtAttrDs. (keep=identity_id) out=ExtIdentities ; by identity_id ; run ;
    proc sort NODUPKEY data=&FinalIntAttrDs. (keep=identity_id) out=IntIdentities ; by identity_id ; run ;

    data IdentityMerge ;
      merge IntIdentities (in=inIntflag)
            ExtIdentities (in=inextflag)       
      ;
      by identity_id ; 
      if not (first.identity_id and last.identity_id) then abort ;
      inExt = (inExtflag) ;
      inInt = (inIntFlag) ;
    run ;

    proc freq data=IdentityMerge noprint ;
      tables inExt*inInt / missing out=IdentityOverlap ;
    run ;

    title "Unique Identity_id overlap between internal and external input data" ;
    proc print data=IdentityOverlap ;
      format count comma12. percent comma8.2 ;
    run ;
  %end ;

  proc freq data=combined noprint ;
    tables inExt*inInt*interaction_type / missing out=typeComp0 ;
    tables inExt*inInt*date / missing out=dateComp0 ;
    tables inExt*inInt*interaction / missing out=IntComp0;
    tables inExt*inInt*interaction_subtype / missing out=IntSubComp0;
  run ;

  proc freq data=combined (where=(inInt=1 AND interaction_type="Conversion")) noprint ;
    tables interaction_id / missing out=Dataviews ;
  run ;

  title "List of dataview IDs associated with conversions in the internal attribution data" ;
  proc print data=Dataviews ;
    format count comma12. percent comma12.2 ;
  run ;

  ** Compare interaction_type **;
  proc sort data=typeComp0 (where=(inExt=1)) out=extType (drop=inExt inInt rename=(count=ExtCount percent=ExtPct)) ; by interaction_type ; run ;
  proc sort data=typeComp0 (where=(inInt=1)) out=intType (drop=inExt inInt rename=(count=IntCount percent=IntPct)) ; by interaction_type ; run ;

  data typeComp ;
    merge extType (in=inext)
          intType (in=inInt) end=lastrec
    ;
    by interaction_type ;
    if not (first.interaction_type and last.interaction_type) then abort ;
    retain Interaction_Type_Err 0 ;
    if interaction_type not in ("Origination" "Conversion" "Task") then Interaction_Type_Err = 1 ;
    if lastrec then call symput("Interaction_Type_Err",put(Interaction_Type_Err,8.)) ;
    format ExtCount IntCount comma12. ExtPct IntPct comma8.2 ;
  run ;

  title "Overlap of interaction_type values between internal and external input data" ;
  proc print data=typeComp ;
  run ;

   ** Compare interaction_subtype **;
  proc sort data=IntSubComp0 (where=(inExt=1)) out=extSubType (drop=inExt inInt rename=(count=ExtCount percent=ExtPct)) ; by interaction_subtype ; run ;
  proc sort data=IntSubComp0 (where=(inInt=1)) out=intSubType (drop=inExt inInt rename=(count=IntCount percent=IntPct)) ; by interaction_subtype ; run ;

  data IntSubtypeComp ;
    merge extSubType (in=inext)
          intSubType (in=inInt) end=lastrec
    ;
    by interaction_subtype ;
    if not (first.interaction_subtype and last.interaction_subtype) then abort ;
    format ExtCount IntCount comma12. ExtPct IntPct comma8.2 ;
  run ;

  title "Commom interaction_subtype values between internal and external input data: See WORK.IntSubtypeComp for all %nobs(IntSubtypeComp) values" ;
  proc print data=IntSubtypeComp (where=(intCount ne . and extCount ne .)) ;
  run ;

  ** Compare interaction **;
  proc sort data=IntComp0 (where=(inExt=1)) out=extInteraction (drop=inExt inInt rename=(count=ExtCount percent=ExtPct)) ; by interaction ; run ;
  proc sort data=IntComp0 (where=(inInt=1)) out=intInteraction (drop=inExt inInt rename=(count=IntCount percent=IntPct)) ; by interaction ; run ;

  data InteractionComp ;
    merge extInteraction (in=inext)
          intInteraction (in=inInt) end=lastrec
    ;
    by interaction ;
    if not (first.interaction and last.interaction) then abort ;
    format ExtCount IntCount comma12. ExtPct IntPct comma8.2 ;
  run ;

  title "Common interaction values between internal and external input data: See WORK.InteractionComp for all %nobs(InteractionComp) values" ;
  proc print data=InteractionComp (where=(intCount ne . and extCount ne .)) ;
  run ;

  ** compare date **;
  proc sort data=dateComp0 (where=(inExt=1)) out=extDate (drop=inExt inInt rename=(count=ExtCount percent=ExtPct)) ; by date ; run ;
  proc sort data=dateComp0 (where=(inInt=1)) out=intDate (drop=inExt inInt rename=(count=IntCount percent=IntPct)) ; by date ; run ;

  data dateComp ;
    merge extDate (in=inext)
          intDate (in=inInt)
    ;
    by date ;
    if not (first.date and last.date) then abort ;
    format ExtCount IntCount comma12. ExtPct IntPct comma8.2 ;
  run ;

  title "Overlapping Date values (of %nobs(dateComp) unique values in WORK.datecomp) between internal and external input data" ;
  proc print data=dateComp (where=(intCount ne . and extCount ne .)) ;
  run ;

  %if "&Interaction_Type_Err." = "1" %then %do ;
    %put Error: Unexpected values found for Interaction_type. Acceptable values are: Origination, Conversion, and Task ;
    %goto FINISH ;
  %end ;

  *------------------------------------------------------------------------------------*;
  *             Create delimited text Export file and compress the file                *;
  *------------------------------------------------------------------------------------*;

  proc format;
    picture POSIX other='%0Y-%0m-%0d %0H:%0M:%0s' (datatype=datetime);
  run;

  filename txtout "&OutfileName." ;

  data _null_;
    length task_id load_id creative_id $36. interaction_cost 8. ;
    set combined ;
    if interaction_cost = . then interaction_cost = 0 ;
    file txtout LRECL=30000;
    length _aLine_ $30000. ;
    _aLine_ = cat(
      strip(identity_id),&dlm.,
      strip(interaction_type),&dlm.,
      strip(put(interaction_id,$36.)),&dlm.,
      strip(interaction),&dlm.,
      strip(interaction_subtype),&dlm.,
      strip(put(interaction_dttm,POSIX.)),&dlm.,
      strip(task_id),&dlm.,
      strip(put(conversion_value,8.)),&dlm.,
      strip(put(interaction_cost,8.)),&dlm.,
      strip(load_id),&dlm.,
      strip(creative_id)
    );
    _aLine_=strip(_aLine_);
    put _aLine_;
  run;

  X gzip "&OutfileName.";

  %FINISH:
%mend ;