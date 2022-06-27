/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : make_EO_data_from_direct_export.sas
/ Version   : 2.1
/ Author    : Noah Powers
/ Created   : 2019 August
/ LastModBy : Noah Powers
/ LastModDt : 03.04.2021
/ Purpose   : This stored process when run as a POST Process in the direct task,  creates the 
/             standard MO/EO datasets from an CI360 Direct
/             Export file associated with a direct task that contains the following colums:
/               - Customer ID 
/               - zero or more customer attributes needed for optimization
/               - zero or more model scores assocaited with offers/messages
/               - SEG_Cd
/               - SEG_ID
/               - Seg_Nm
/               - TSK_Cd
/               - TSK_ID
/               - TSK_Nm
/               - TSK_VERID
/               - [task custom attribute name]_TSKAttr
/               - Optimization_Time_Period_TSKAttr (TimeAggTypeVar macro var)
/               - Exp_Delivery_Date_TSKAttr        (DeliveryDateVar macro var)
/               - Optimization_Group_Name_TSKAttr  (OptGroupVar macro var)
/               - MSG_Cd
/               - MSG_ID
/               - MSG_Nm
/               - [message custom attribute name]_MSGAttr_[different string for each message]
/           
/             Output Datasets:
/             - MO_Customers
/             - MO_Campaigns 
/             - MO_Communications 
/             - MO_Control 
/             - MO_Time_Periods 
/ FuncOutput: N/A
/ Usage     :
/ Notes     : - This code assumes the export file is a SAS dataset where the export location 
/               is in the folder provided in the CIEXPORTPATH macro variable below.
/             - This code assumes that only one message per segment in the task Targeting tab
/             - Segments in the direct task should represent eligible customers for the associated message and 
/               0/1 segment inclusion indicator columns will be added to MO_Customers for each 
/               segment, using the segment code (e.g. SEG_199) as the variable name.
/             - This code only works for certain time aggregate values: daily, week beginning {dow}, monthly
/               that were set up as a drop down list in the task custom properties
/             - This code has hard coded values for three different libraries in SMC but can 
/               easily be extended to more.
/             - Make sure that every numeric variable in the export file has a valid SAS format 
/               associated with it in the Export Content tab of the direct task.
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name              Description
/ -------------------------------------------------------------------------------------
/ CustIDVarName   = The output name (in the export file) of the column in the export file that represents
/                   the customer ID.
/ TimeAggTypeVar  = (default=Optimization_Time_Period_TSKAttr)  This is the output name of the column
/                   in the export file that captures the time aggregate type (e.g. week beginning monday)
/ DeliveryDateVar = (default=Exp_Delivery_Date_TSKAttr) This is the output name of the colunm 
/                   in the export file that caputres the expected delivery date of the communication.
/ OptGroupVar     = (default=Optimization_Group_Name_TSKAttr) This is the output name of the colunm 
/                   in the export file that caputres the optimization group to be used.  The value corresponds
/                   to a previously library object in SMC that is configured for Engage Optimize.
/ ExportDsName    = (default=EO_Customer_Data0) This is the Export Name specified in the direct task to be optimized.
/ MasterControlInDs = User provided library and table name for the master control file.
/ CIExportPath    = The full file path to the location of the export file.
/ DebugListfile   = The full file path and file name to use for the listing file that is created when Debug=Y
/ MetaServer      = metadata server
/ metaport        = metadata port
/ metauser        = user name for metadata server
/ metapass        = password for metadata server
/ Debug           = (Default=N) Set this to Y if the metadata changes need to be better understood.
/============================================================================================*/
%macro make_EO_data_from_direct_export(CustIDVarName     = ,
                                 TimeAggTypeVar    = Optimization_Time_Period_TSKAttr,
                                 DeliveryDateVar   = Exp_Delivery_Date_TSKAttr,
                                 OptGroupVar       = Optimization_Group_Name_TSKAttr,
                                 ExportDsName      = EO_Customer_Data0,
                                 MasterControlInDs = ,
                                 CIExportPath      = ,
                                 DebugListfile     = ,
                                 MetaServer        = ,
                                 metaport          = ,
                                 metauser          = ,
                                 metapass          = ,
                                 Debug             = N) ;

  %local opt_group out_lib_id campVarList renameList CommVarList CustomerVarList 
         BaseVarList NumBaseVars i BaseVar typestr Campaign_cd SegNamesOrdered mo_control ;
 
  libname _cust_ "&CIExportPath." ;

  data _null_ ;
    set _cust_.&ExportDsName. (obs=1);
    call symput("opt_group",strip(&OptGroupVar.)) ;
  run ;

  %if ( "&opt_group." = "group1") %then %do ;
    libname _out_ "D:\CI\Financial Services\Data\EO Optimization Group 1" ;
    %let out_lib_id = A5Z9IMRH.B500002W ;
  %end ;
  %else %if ( "&opt_group." = "group2") %then %do ;
    libname _out_ "D:\CI\Financial Services\Data\EO Optimization Group 2" ;
    %let out_lib_id = A5Z9IMRH.B500002X ;
  %end ;
  %else %if ( "&opt_group." = "group3") %then %do ;
    libname _out_ "D:\CI\Financial Services\Data\EO Optimization Group 3" ;
    %let out_lib_id = A5Z9IMRH.B500002Y ;
  %end ;
  %else %do ;
    %put %str(Error: Unexpected Optimization Group Name found: &opt_group.) ;
    %goto FINISH ;
  %end ;

  proc datasets library=_out_ nolist ;
    delete mo_campaigns mo_communications mo_control mo_time_periods mo_customers ; 
  quit ;
 
  proc contents data=_cust_.&ExportDsName. out=VarList (keep=name type length) noprint ;
  run ;

  data VarList ;
    set VarList ;
    length newName BaseVarName $32. rename $65. ;
    taskAttrLoc = index(upcase(name),"_TSKATTR") ;
    if (taskAttrLoc > 0) AND (upcase(name) not in ("%upcase(&TimeAggTypeVar.)" "%upcase(&DeliveryDateVar.)")) then do ;
       newName = substr(name,1,taskAttrLoc-1) ;
       rename = strip(name) || " = " || strip(newName) ;
    end ;
    if (upcase(name) =: "MSG_") OR (index(upcase(name),"_MSGATTR_") > 0) OR upcase(name) = "TSK_CD" OR  
      (upcase(strip(name)) in ("%upcase(&TimeAggTypeVar.)" "%upcase(&DeliveryDateVar.)")) then CommMeasure = 1 ;
    
    else if (length(name) > 4 AND substr(upcase(name),1,4) = "TSK_") OR (taskAttrLoc > 0) then CampMeasure = 1 ;

    else if substr(upcase(name),1,4) ne "SEG_" then CustomerMeasure = 1 ; 

    msg_attr_loc = index(upcase(name),"_MSGATTR_") ;
    if msg_attr_loc > 0 then do ;
      BaseVarName = substr(name,1,msg_attr_loc-1) ;
      Message_cd = "MSG" || substr(name,msg_attr_loc+8) ;
    end ;

    drop taskAttrLoc msg_attr_loc ;
  run ;

  proc sql noprint ;
    select name into: campVarList separated by " " from VarList where CampMeasure = 1 or upcase(name) = "TSK_CD" ;
    select distinct rename into: renameList separated by " " from VarList ;
    select distinct name into: CommVarList separated by " " from Varlist where CommMeasure = 1 ;
    select distinct name into: CustomerVarList separated by " " from Varlist where CustomerMeasure = 1 ;
  quit ;

  proc sort NODUPKEY data=Varlist (keep=BaseVarName length where=(BaseVarName ne " ")) out=BaseVars ;
    by BaseVarName ;
  run ;

  proc sql noprint ;
    select distinct BaseVarName into: BaseVarList separated by " " from BaseVars ;
    select count(*) into: NumBaseVars from BaseVars where baseVarName ne " " ;
  quit ;

  data _null_ ;
    set Basevars;
    call symput(strip(baseVarName)||"len",strip(put(length,8.))) ;
  run ;

  %do i = 1 %to &NumBaseVars. ;
    %let BaseVar = %scan(&BaseVarList.,&i.,%str( )) ;
    %local BaseList&i. BaseVarType&i. ;
    proc sql noprint ;
      select distinct name into: BaseList&i. separated by " " from VarList where basevarname = "&BaseVar." ;
      select distinct type into: BaseVarType&i. separated by " " from VarList where basevarname = "&BaseVar." ;
    quit ;
  %end ;

  %put &campVarList. ;
  %put &renameList. ;

  data MO_Campaigns ;
    length campaign_cd $25. ;
    set _cust_.&ExportDsName. (obs=1 keep=&campVarList.) ;
    campaign_cd = strip(tsk_cd) ;
    call symput("Campaign_cd",strip(campaign_cd)) ;
    rename &renameList. ;
    drop tsk_cd ;
  run ;

  proc sort NODUPKEY data=_cust_.&ExportDsName. (keep=&CommVarList.) out=Communication0 ;
    by msg_cd ; 
  run ;

  data MO_Communications ;
    length Campaign_cd $25. Communication_cd $25. time_period $15. ;
    set Communication0 ;

    campaign_cd = strip(tsk_cd) ;
    communication_cd = strip(tsk_cd) || "_" || strip(msg_cd) ;
    
    %do i = 1 %to &NumBaseVars. ;
      %let BaseVar = %scan(&BaseVarList.,&i.,%str( )) ;
      %if (&&BaseVarType&i. = 2) %then %let typestr = $ ;
      array bv&i. (*) &typestr. &&BaseList&i. ;
      length &BaseVar. &typestr. &&&BaseVar.Len. ;

      do i = 1 to dim(bv&i.) ;
        if NOT missing(bv&i.(i)) then &BaseVar. = bv&i.(i) ;
      end ;
    %end ;

    if &TimeAggTypeVar. = "Daily" then time_period = put(&DeliveryDateVar.,yymmdd10.) ;
    else if &TimeAggTypeVar. = "Monthly" then time_period = put(&DeliveryDateVar.,MONYY7.) ;
    else do ;
      delivery_dow = weekday(&DeliveryDateVar.) ;
      select ;
        when (&TimeAggTypeVar.="WB_Sunday")     opt_dow = 1 ;
        when (&TimeAggTypeVar.="WB_Monday")     opt_dow = 2 ;
        when (&TimeAggTypeVar.="WB_Tuesday")    opt_dow = 3 ;
        when (&TimeAggTypeVar.="WB_Wednesday")  opt_dow = 4 ;
        when (&TimeAggTypeVar.="WB_Thursday")   opt_dow = 5 ;
        when (&TimeAggTypeVar.="WB_Friday")     opt_dow = 6 ;
        when (&TimeAggTypeVar.="WB_Saturday")   opt_dow = 7 ;
        otherwise abort ;
      end ;

      time_period = put(&DeliveryDateVar. + opt_dow - delivery_dow -7*(opt_dow > delivery_dow),yymmdd10.) ;
    end ;

    drop tsk_cd i %do i = 1 %to &NumBaseVars.; &&BaseList&i. %end ; delivery_dow opt_dow &TimeAggTypeVar. ;
  run ;

  proc sort NODUPKEY data=MO_Communications (keep=time_period &DeliveryDateVar.) out=Time_Periods0 ;
    by &DeliveryDateVar. ;
  run ;

  data MO_Time_Periods ;
    set time_periods0 ;
    order_no = _N_ ;
    drop &DeliveryDateVar. ;
  run ;

  %if %sysfunc(exist(&MasterControlInDs.)) %then %do ;
    data MO_Control ; 
      *length Campaign_cd $25. Communication_cd $25. ;
      set &MasterControlInDs. ;
      where campaign_cd = "&Campaign_cd." ;
    run ;

    %let mo_control = mo_control ;
  %end ;
  %else %do ;
    %put %str(Warning: Control input data not found in &MasterControlInDs.) ;
    %put ;
    %let mo_control = ;
  %end ;

  data MO_Customer_Vars (compress=YES);
    set _cust_.&ExportDsName. (keep=&CustomerVarList.) ;
    by &CustIDVarName. ;
    if first.&CustIDVarName. ;
  run ;

  data Customer_Segments0 (compress=yes) ;
    set _cust_.&ExportDsName. (keep=&CustIDVarName. SEG_:) ;
    InSegment = 1 ;
  run ;

  proc transpose data=Customer_Segments0 out=Customer_Segments0_T (drop=_name_) ;
    by &CustIDVarName. ;
    id seg_cd ;
    idlabel seg_nm ;
    var InSegment ;
  run ;

  proc sql noprint ;
    select distinct name into: SegNamesOrdered separated by " " from sashelp.vcolumn
    where upcase(libname) = "WORK" AND upcase(memname) = "CUSTOMER_SEGMENTS0_T" AND upcase(name) like "SEG_%" ;
  quit ;

  data Customer_Segments0_T ;
    retain &SegNamesOrdered. ;
    set Customer_Segments0_T ;
  run ;

  data MO_Customers (compress=yes) ;
    merge MO_Customer_Vars     (in=inmain)
          Customer_Segments0_T (in=inseg)
    ;
    by &CustIDVarName. ;
    if not (first.&CustIDVarName. and last.&CustIDVarName.) then abort ;
    if not (inmain and inseg) then abort ;

    array seg (*) &SegNamesOrdered. ;
    do i = 1 to dim(seg) ;
      if seg(i) = . then seg(i) = 0 ; 
    end ;

    drop i ;
  run ;

  proc copy in=work out=_out_ ;
    select mo_campaigns mo_communications mo_time_periods mo_customers &mo_control.;
  run ;

  %if ("&debug." = "Y") %then %do ;
  
    proc printto print="&DebugListfile." NEW ;
    run ;
      
    options metaserver="&MetaServer."
      metaport=&metaport.
      metauser="&metauser."
      metapass="&metapass."
      metarepository=Foundation
      metaprotocol=BRIDGE;

    options sastrace=',,,d' sastraceloc=saslog nostsuffix;

    proc metalib TL=16383;
       omr(liburi="&out_lib_id." repname="Foundation");
       select (mo_campaigns mo_communications mo_time_periods mo_customers &mo_control.);
       report(type = DETAIL);
    run;

  %end ;
  
  proc metalib;
    omr (liburi="&out_lib_id." repname="Foundation" server="&MetaServer." user="&metauser." metapass="&metapass.");
    select (mo_campaigns mo_communications mo_time_periods mo_customers &mo_control.) ;
    update_rule (DELETE) ;
  run;

  libname _cust_ CLEAR ;
  %FINISH: 

%mend ;
