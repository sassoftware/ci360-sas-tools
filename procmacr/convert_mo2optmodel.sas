/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/===============================================================================
/ Program   : Convert_MO2OptModel.sas
/ Version   : $Revision: 1 $
/ Author    : Noah Powers
/ LastModBy : $Author: noah $
/ Date      : 02.28.2015
/ LastMod   : $Date: $
/ SubMacros : [external] %vartype %varlen %hasvars %nobs %quotelst attrv
/ Purpose   : Create text file with proc optmodel code that is equivalent to 
/             an MO problem from the batch files.
/ Notes     : Does NOT support: secondary objectives, control group parameters 
/ Usage     : 
/===============================================================================
/ PARAMETERS:
/-------name------- -------------------------description------------------------
/ batch_MO_lib           (required) SAS library name for the exported batch files 
/ MO_inds_lib            (required) SAS MO input data library reference
/ MO_Customer_Table_Name (required) Name of MO customer table (in MO_inds_lib)
/ MO_Control_Table_Name  (required) Name of MO control table (in MO_inds_lib)
/ outfilename            (required) SAS filename for the output proc optmodel code
/ mkdatafilename         (required) SAS filename for the data views leveraged in optmodel code
/ Outlib                 SAS libname for output data used by proc optmodel code
/ SolutionOutds          Name of the solution dataset created by proc optmodel code
/ CustomerDataOutView    Name for the customer data (view) created for proc optmodel code
/ OfferDataOutDs         Name for the offer data created for proc optmodel code
/ HouseholdOutView       Name for the household view (unqiue list hhs) created for proc optmodel code
/ ContactHistoryOutView  Name for the Contact History data view
/ CustObs=MAX            # obs to read from MO Customer table (for testing)
/ OutFileRecLen          Max line Length for output text file
/===============================================================================
/ AMENDMENT HISTORY:
/ init --date-- mod-id ----------------------description------------------------
/=============================================================================*/
%macro Convert_MO2OptModel(batch_MO_lib=,
                           MO_inds_lib=,
                           MO_Customer_Table_Name=,
                           MO_Control_Table_Name=,
                           outfilename=,
                           mkdatafilename=,
                           Outlib=WORK,
                           SolutionOutds=OptMod_solution,
                           CustomerDataOutView=_customerView_,
                           OfferDataOutDs=_offers_,
                           HouseholdOutView=_hhsView_,
                           ContactHistoryOutView=_ContactHistoryView_,
                           CustObs=MAX,
                           OutFileRecLen=130
                           ) ;

  options NOQUOTELENMAX ;
  
  %macro ConvertFormulas(inds             =,
                         outds          =&inds.,
                         source_txt_var =,
                         dest_txt_var   =&source_txt_var.) ;

    data &outds. ;
      set &inds. ;
      %if NOT %hasvars(&inds.,&dest_txt_var.) %then %do ;
        length &dest_txt_var. $2048. ;
      %end ;

      length _tmp_var_ $2048. ;
      _tmp_var_ = trim(left(&source_txt_var.)) ;
      %include fConv ;
      
      &dest_txt_var. = _tmp_var_ ;
      drop _tmp_var_ ;
    run ;
  %mend ;
  
  %macro add_spaces(inds=,outds=&inds.,source_txt_var=,dest_txt_var=&source_txt_var.) ;
    data &outds. ;
      set &inds. ;
      length _tmp_var_ $2048. ;
      array symbols (15) $2 _temporary_ ("+" "-" "/" "*" "(" ")" "~=" "=~" "<=" ">=" "=<" "=>" "<" ">" "=") ;
      
      _tmp_var_ = trim(left(&source_txt_var.)) ;
      if trim(left(&source_txt_var.)) > "" then do i = 1 to dim(symbols) ;
        _tmp_var_ = tranwrd(_tmp_var_,trim(symbols(i))," "||trim(symbols(i))||" ") ;
      end ;
      _tmp_var_ = tranwrd(_tmp_var_,"<  =","<=") ;
      _tmp_var_ = tranwrd(_tmp_var_,">  =",">=") ;
      _tmp_var_ = tranwrd(_tmp_var_,"=  <","=<") ;
      _tmp_var_ = tranwrd(_tmp_var_,"=  >","=>") ;
      _tmp_var_ = tranwrd(_tmp_var_,"=  ~","=~") ;
      _tmp_var_ = tranwrd(_tmp_var_,"~  =","~=") ;

      &dest_txt_var. = _tmp_var_ ;
      drop i _tmp_var_ ;
    run ;
  %mend ;

  data _null_ ;
    file &mkdatafilename. LRECL=1000 ;
    length line $1000. ;
    %if ("%upcase(&Outlib.)" NE "WORK") %then %do ; 
      line = "libname &outlib. "  || '"' || "%sysfunc(pathname(&outlib.,L))" || '";' ; 
      put line ;
      line = " " ;
      put line ;
    %end ;
    line = "libname &MO_inds_lib. "  || '"' || "%sysfunc(pathname(&MO_inds_lib.,L))" || '";' ; 
    put line ;
    line = " " ;
    put line ;
  run ;

  %local customer_id comm_num_var_list comm_char_var_list NumOffers Num_Var_Cnt Char_Var_Cnt i j var CustAttr 
         min_time_period_no max_time_period_no has_time_periods Comm_Table_Drop_Vars offer_set 
         Cust_level_vars Comm_level_vars CustComm_level_vars calc_vars has_hh_constraint1 has_hh_constraint2
         has_hh_constraint ;

  %let Comm_Table_Drop_Vars = CONTROL_GROUP_BEHAVIOR_OPTION CONTROL_GROUP_PERCENT 
                   CONTROL_GROUP_SIZE EXCLUDE_FROM_CONSTRAINTS EXCLUDE_FROM_CONTACT_POLS MANDATORY OFFER_ID ;
  %let has_hh_constraint1 = 0 ;
  %let has_hh_constraint2 = 0 ;
 
  proc sql noprint ;
    select upcase(customer_id) into: customer_id separated by " " from &batch_MO_lib..scenario_parameters ;
    select upcase(household_id) into: household_id separated by " " from &batch_MO_lib..scenario_parameters ;
    select trim(left(upcase(objective_numeric_measure))) into: objective_measure from &batch_MO_lib..scenario_parameters ;
    select sample_ratio into: sample_ratio from &batch_MO_lib..scenario_parameters ;
    select sample_seed into: sample_seed from &batch_MO_lib..scenario_parameters ;
    select active_agent_constraints_flg into: active_agent_constraints_flg from &batch_MO_lib..scenario_parameters ;
    select agent_keyword into: agent_keyword from &batch_MO_lib..scenario_parameters ;
    select active_contact_history_flg into: active_contact_history_flg from &batch_MO_lib..scenario_parameters ;
    select contact_history_table_name into: contact_history_table_name from &batch_MO_lib..scenario_parameters ;
    select communication_history_table_name into: communication_history_table_name from &batch_MO_lib..scenario_parameters ;
  quit ;
  %let agent_keyword = %trim(%left(&agent_keyword.)) ;
  %let sample_ratio = %trim(%left(&sample_ratio.)) ;
  %let sample_seed = %trim(%left(&sample_seed.)) ;

  %if "&household_id." ne "" %then %do ;
    %if (%upcase(%vartype(&MO_inds_lib..&MO_Customer_Table_Name.,&household_id.)) = C) %then %do ;
      %let hhdecl = <String> HOUSEHOLDS ;
    %end ;
    %else %do ;
      %let hhdecl = HOUSEHOLDS ;
    %end ;
  %end ;

  ** Dont bring in contact history unless time based policies**;
  data _null_ ;
    set &batch_MO_lib..CONTACT_POLICIES end=lastrec ;
    length has_time_based_policy 3. ;
    retain has_time_based_policy 0 ;
    if rolling_period_cnt NE . then has_time_based_policy = 1 ;
    if lastrec then do ;
      if NOT has_time_based_policy then call symput("active_contact_history_flg","0") ;
    end ;
  run ;

  data comms ;
    length campaign_cd %varlen(&MO_inds_lib..&MO_Control_Table_Name.,campaign_cd)
           communication_cd %varlen(&MO_inds_lib..&MO_Control_Table_Name.,communication_cd) ;
    set &batch_MO_lib..communications (in=innew rename=(campaign_cd=campaign_cd0001 communication_cd=communication_cd0001)) 
    %if (&active_contact_history_flg. = 1) %then %do ;
        &MO_inds_lib..&communication_history_table_name. (in=inold rename=(campaign_cd=campaign_cd0001 communication_cd=communication_cd0001)) 
    %end ;;

    offer_id = _N_ ;
    campaign_cd = trim(left(campaign_cd0001)) ;
    communication_cd = trim(left(communication_cd0001)) ;
  
    if upcase(channel_cd) = "%upcase(&agent_keyword.)" then 
      call symput("agent_keyword",trim(left(channel_cd))) ;
    *channel_cd = upcase(channel_cd) ;
    drop campaign_cd0001 communication_cd0001 ; 
  run ;

  %let has_time_periods = 0 ;
  %if (%nobs(&batch_MO_lib..time_periods) > 0) %then %do ;
    %let has_time_periods = 1 ;
    data time_period_format ;
      set &batch_MO_lib..time_periods (rename=(order_no=label time_period=start)) ;
      length fmtname $7. type $1. ;
      retain fmtname "timenum" ;
      type = "I" ;
    run ;

    proc format CNTLIN=time_period_format ;
    run ; 

    data comms ;
      set comms ;
      time_period_order_no = input(time_period,timenum.) ;
    run ;

    proc sql noprint ;
      select max(time_period_order_no) into: max_time_period_no from comms ;
      select min(time_period_order_no) into: min_time_period_no from comms ;
    quit ;
  %end ;

  data &Outlib..&OfferDataOutDs. ;
    set comms ;
  run ;

  %if (&active_contact_history_flg. = 1) %then %do ;
    data &Outlib..Hist_Comm_format ;
      set comms (where=(start NE .) rename=(offer_id=label UNIQUE_COMM_MAPPING_ID=start)) ;
      length fmtname $7. ;
      retain fmtname "HC_Map" ;
      *type = "N" ;
    run ;

    proc format CNTLIN=&Outlib..Hist_Comm_format ;
    run ; 

    data _null_ ;
      file &mkdatafilename. LRECL=1000 MOD ;
      length line $1000. ;
      line = "proc format CNTLIN=&Outlib..Hist_Comm_format ;" ;
      put line ;
      line = "run ; " ;
      put line ;
      line = " " ;
      put line ;
    run ;

    data comms ;
      set &Outlib..&OfferDataOutDs. ;
      if unique_comm_mapping_id = . ;
    run ;
  %end ;

  ** Create set for Offers inluded in constraints IF not all OFFERS **;
  %local Comms_Excl_Const Comms_Excl_ConPoly Mandatory_Comms ;
  proc sql noprint ;
    select distinct offer_id into: Comms_Excl_Const separated by "," from comms (where=(exclude_from_constraints = 1)) ;
    select distinct offer_id into: Comms_Excl_ConPoly separated by "," from comms (where=(exclude_from_contact_pols = 1)) ;
    select distinct offer_id into: Mandatory_Comms separated by "," from comms (where=(Mandatory = 1)) ;
  quit ;

  %if %length(&Comms_Excl_Const.) %then %let OFFERS_W_CONS_Set = OFFERS_W_CONS ;
    %else %let OFFERS_W_CONS_Set = OFFERS ;
  %if %length(&Comms_Excl_ConPoly.) %then %let OFFERS_W_POLICY_Set = OFFERS_W_POLICY ;
    %else %let OFFERS_W_POLICY_Set = OFFERS ;

  proc datasets library=work nolist ;
    modify Comms ;
    index create campcomm=(campaign_cd communication_cd) / UNIQUE ;
  quit ;

  proc contents data=comms noprint out=comm_var_list ;
  run ;

  proc contents data=&MO_inds_lib..&MO_Customer_Table_Name. noprint out=cust_var_list ;
  run ;

  proc sql noprint ;
    select upcase(name) into: comm_num_var_list separated by " " from comm_var_list (where=(type=1 and upcase(name) NOT in (%quotelst(&Comm_Table_Drop_Vars.)))) ;
    select upcase(name) into: comm_char_var_list separated by " " from comm_var_list (where=(type=2 and upcase(name) NOT in (%quotelst(&Comm_Table_Drop_Vars.)))) ;
    select count(*) into: NumOffers from comms ;
    select count(*) into: Num_Var_Cnt from comm_var_list (where=(type=1 and upcase(name) NE "OFFER_ID")) ;
    select count(*) into: Char_Var_Cnt from comm_var_list (where=(type=2)) ;
    select upcase(name) into: cust_num_var_list separated by '" "' from cust_var_list (where=(type=1 and upcase(name) NE "&customer_id.")) ;
    select upcase(name) into: cust_char_var_list separated by '" "' from cust_var_list (where=(type=2 and upcase(name) NE "&customer_id.")) ;
  quit ;

  data measures0 ;
    set &batch_MO_lib..numeric_measures (where=(trim(left(numeric_measure)) > "")) end=lastrec;
    numeric_measure = upcase(trim(left(numeric_measure))) ;
    if numeric_measure_type_cd = "CUSTATTR" then numeric_measure_type_cd = "CUSTNUM" ;
    output ;
    %if "%trim(&active_agent_constraints_flg.)" = "1" %then %do ;
      if lastrec then do ;
        numeric_measure = "&agent_keyword." ;
        %if (%upcase(%vartype(&MO_inds_lib..&MO_Customer_Table_Name.,&agent_keyword.)) = C) %then %do ;
          numeric_measure_type_cd = "CUSTCHAR" ;
        %end ;
        %else %do ;
          numeric_measure_type_cd = "CUSTNUM" ;
        %end ;
        output ;
      end ;
    %end ;
    rename numeric_measure = measure ;
  run ;

  data measures1 ;
    set &batch_MO_lib..solution_variables ;
    variable_name = upcase(trim(left(variable_name))) ;
    rename variable_name=measure ;
  run ;

  data measures2 ;
    set comm_var_list ;
    length measure $32. numeric_measure_type_cd $16. ;
    measure = trim(left(upcase(name))) ;
    if type = 2 then numeric_measure_type_cd = "COMMCHAR" ;
    else if type = 1 then numeric_measure_type_cd = "COMMNUM" ;
    else abort ;
    keep measure numeric_measure_type_cd ;
  run ;

  data suppression_measures ;
    length varlist $1000. ;
    retain varlist ;
    set batch.suppression_rules (where=(active_flg = 1)) end=lastrec ;
    %do i = 1 %to %words("&cust_char_var_list.") ;
      %let cust_char_var = %scan("&cust_char_var_list.",&i.,%str( )) ;
      if index(upcase(customer_filter_txt),&cust_char_var.) > 0 then varlist = trim(left(varlist)) || " " || &cust_char_var. ;
    %end ;
    if lastrec then do ;
      call symput("Supp_customer_char_vars",trim(left(varlist))) ;
    end ;
  run ;

  data measures3 ;
    length measure $32. measure_type_cd $16. ;
    set cust_var_list (where=(type=2)) ;
    name = upcase(name) ;
    measure_type_cd = "CUSTCHAR" ;
    if name in (%quotelst(&Supp_customer_char_vars.)) then do ;
      measure = trim(left(name)) ;
      output ;
    end ;    
    keep measure measure_type_cd ;
  run ;

  proc sort data=measures0 ; by measure ; run ;
  proc sort data=measures1 ; by measure ; run ;
  proc sort data=measures2 ; by measure ; run ;
  proc sort data=measures3 ; by measure ; run ;

  ** Combine Numeric Measures and all comm level vars **;
  data measures ;
    merge measures0 (in=inmain) 
          measures2 (in=incomm rename=(numeric_measure_type_cd=measure_type_cd0))
    ;
    by measure ;
    formula_txt = trim(left(formula_txt)) ;
    if incomm then numeric_measure_type_cd=measure_type_cd0 ;
    rename numeric_measure_type_cd=measure_type_cd ;
    drop measure_type_cd0 ;
  run ;

  data measures ;
    merge measures  (in=innum)
          measures1 (in=inoth)
          measures3 (in=insupp)
    ;
    by measure ;
    if not (first.measure and last.measure) then abort ;
    if inoth and not innum then do ;
      if measure in ("&cust_char_var_list.") then measure_type_cd = "CUSTCHAR" ;
      else if measure in ("&cust_num_var_list.") then measure_type_cd = "CUSTNUM" ;
      else abort ;
    end ;
    outputvar = 0 ;
    if inoth OR (upcase(measure) in ("CAMPAIGN_CD" "COMMUNICATION_CD" "TIME_PERIOD" "CHANNEL_CD")) then outputvar = 1 ;
    if measure in (%quotelst(&Comm_Table_Drop_Vars.)) then delete ;
    *if measure in (%quotelst(&comm_num_var_list. &comm_char_var_list.)) then delete ;
    *if inoth and not innum and measure NOT in (%quotelst(&comm_num_var_list. &comm_char_var_list.)) then 
      numeric_measure_type_cd = "CUSTCHAR" ;
  run ;

  %let foundall = 0 ;
  %let iteration = 1 ;
  %do %until (&foundall. = 1) ;
  
    proc sql noprint ;
      select distinct upcase(measure) into: Cust_level_vars separated by '" "' from measures (where=(measure_type_cd in ("CUSTNUM" "CUSTCHAR"))) ;
      select distinct upcase(measure) into: Comm_level_vars separated by '" "' from measures (where=(measure_type_cd in ("COMMCHAR" "COMMNUM"))) ;
      select distinct upcase(measure) into: CustComm_level_vars separated by '" "' from measures (where=(measure_type_cd in ("CUSTCOMM"))) ;
      select distinct upcase(measure) into: calc_vars separated by '" "' from measures (where=(measure_type_cd in ("CALC" "COMMFILTER" "CUSTFILTER"))) ;
    quit ;

    %put Cust_level_vars="&Cust_level_vars." ;
    %put Comm_level_vars="&Comm_level_vars." ;
    %put CustComm_level_vars="&CustComm_level_vars." ;
    %put calc_vars="&calc_vars." ;

    data measures (keep=formula_txt measure measure_type_cd outputvar CalcOrder) 
         chk&iteration. ;
      set measures end=lastrec ;
      length DependentVars $200. new_formula_txt $2048. _chr_ $1. ;
      retain foundall 1 ;
      done = 0 ;
      CustLvl = 0 ;
      CommLvl = 0 ;
      CustCommLvl = 0 ;
      word = 1 ;
      formula_txt = trim(left(compbl(formula_txt))) ;
      if measure_type_cd in ("COMMFILTER" "CUSTFILTER")then do ;
        measure_type_cd = "CALC" ;
        formula_txt = "(" || trim(left(formula_txt)) || ")" ;
      end ;
      if trim(left(formula_txt)) > "" AND measure_type_cd = "CALC" then do until (done) ;
        var = upcase(scan(formula_txt,word,"+-/*><=() {}"))  ; 
        if var <= " " then done = 1 ;
        else do ;
          if var in ("&Cust_level_vars.") then CustLvl = 1 ;
          else if var in ("&Comm_level_vars.") then CommLvl = 1 ;
          else if var in ("&CustComm_level_vars.") then CustCommLvl = 1 ;
          else if var in ("&calc_vars.") then DependentVars = trim(left(DependentVars)) || trim(left(var)) ;
          else if (substr(var,1,1) NOT in ('"' "0" "1" "2" "3" "4" "5" "6" "7" "8" "9")) AND (var ^= "IN") then abort ;
          word = word + 1 ;
        end ;
        CalcOrder = &iteration. ;
      end ;
      if trim(left(DependentVars)) > "" then foundall = 0 ;
      else if CustCommLvl = 1 OR (CustLvl = 1 AND CommLvl = 1) then measure_type_cd = "CUSTCOMM" ;
      else if CustLvl = 1 then measure_type_cd = "CUSTNUM" ;
      else if CommLvl = 1 then measure_type_cd = "COMMNUM" ;

      if (&iteration. <= 1) then do ;
        ** Convert formula to uppercase except for text literals **;
        write_upcase = 1 ;
        do _pos_ = 1 to length(formula_txt) ;
          _chr_ = substr(formula_txt,_pos_,1) ;
          if _chr_ in ("'" '"') then write_upcase = NOT write_upcase ;
          if write_upcase then substr(new_formula_txt,_pos_,1) = upcase(_chr_) ;
          else substr(new_formula_txt,_pos_,1) = _chr_ ;
        end ;
        formula_txt = trim(left(new_formula_txt)) ;

        ** optmodel IN clause needs { } instead of ( ) **;
        ** this code will fail if there are text literals in IN clause with () in them**;
        substr_start_pos = 1 ;
        _pos_ = index(substr(formula_txt,substr_start_pos)," IN ") ;
        str_length = length(trim(left(formula_txt))) ;
        do while (_pos_ > 0) ;
          _pos_start_ = _pos_ + 3 + index(substr(formula_txt,_pos_+4),"(") ;
          _pos_end_ = _pos_start_ + index(substr(formula_txt,_pos_start_+1),")") ;
          substr(formula_txt,_pos_start_,1) = "{" ;
          substr(formula_txt,_pos_end_,1) = "}" ;
          substr_start_pos = _pos_end_ + 1 ;
          if substr_start_pos <= str_length then _pos_ = index(substr(formula_txt,substr_start_pos)," IN ") ;
          else _pos_ = 0 ;
        end ;
      end ;
          
      file log ;
      put _ALL_ ;
      if lastrec then call symput("foundall",trim(left(put(foundall,8.)))) ;
    run ;

    %let iteration = %eval(&iteration. + 1) ;
  %end ;
 
  data measures ;
    set measures ;
    length array_ref $40. ;
    if measure_type_cd in ("CUSTCHAR" "CUSTNUM") then array_ref = trim(left(measure)) || "[i]" ;
    else if measure_type_cd in ("COMMCHAR" "COMMNUM") then array_ref = trim(left(measure)) || "[j]" ;
    else if measure_type_cd = "CUSTCOMM" then array_ref = trim(left(measure)) || "[i,j]" ;
    else abort ; 
    measure_len = length(trim(left(measure))) ;
  run ;

  %add_spaces(inds=measures,source_txt_var=formula_txt) ;

  proc sort data=measures ; by DESCENDING measure_len ; run ;

  filename fConv "Convert_Formulas.sas" ;
  data _null_ ;
    set measures end=lastrec ;
    length line $2000 ;
    file FConv lrecl=1000 ;
    if (_N_ = 1) then do ;
      line = "done = 0 ;" ;
      put line ;
      line = "word = 1 ;" ;
      put line ;
      line = "do until (done) ;" ;
      put line ; 
      line = "var = upcase(scan(_tmp_var_,word,'+-/*><=() {}'))  ; " ;
      put @3 line ;
      line = "if var <= ' ' then done = 1 ;" ;
      put @3 line ;
      line = "else do ;" ;
      put @3 line ;
      line = "if 1=0 then ;" ;
      put @5 line ;
    end ;
    line = "else if var = '" || trim(left(measure)) || "' then _tmp_var_ = tranwrd(_tmp_var_,'" || trim(left(measure)) || "','" || trim(left(array_ref)) || "') ;" ; 
    put @5 line ;
    if lastrec then do ;
      line = "word = word + 1 ;" ;
      put @5 line ;
      line = "end ;" ;
      put @3 line ;
      line = "end ;" ;
      put line ;
    end ;
  run ;

  proc sort data=measures ; by measure_type_cd ; run ;

  data varDeclarations ; 
    set measures end=lastrec;
    length line $2000 ;
    
    if _N_ = 1 then do ;
      line = " " ;
      output ;
      line = "  ** Declcare all Customer, comm and cust-comm Variable arrays **;" ;
      output ;
    end ;

    %if (&active_contact_history_flg. = 1) %then 
      %let offer_set = FULL_OFFERS ;
    %else 
      %let offer_set = OFFERS ;


    if measure_type_cd = "CUSTNUM" then 
      line = "num " || trim(left(measure)) || " {CUSTOMERS} ;" ;
    else if measure_type_cd in ("CUSTCOMM") then 
      line = "num " || trim(left(measure)) || " {CUSTOMERS, OFFERS} ;" ;
    else if measure_type_cd in ("CUSTCHAR") then 
      line =  "str " || trim(left(measure)) || " {CUSTOMERS} ;" ;
    else if measure_type_cd in ("COMMCHAR") then 
      line =  "str " || trim(left(measure)) || " {&offer_set.} ;" ;
    else if measure_type_cd in ("COMMNUM") then 
      line =  "num " || trim(left(measure)) || " {&offer_set.} ;" ;
    else abort ;
    *put line ;
    output ;

    if lastrec then do ;
      %if "&household_id." ne "" %then %do ;
        %if (%upcase(%vartype(&MO_inds_lib..&MO_Customer_Table_Name.,&household_id.)) = C) %then %do ;
          %let hhmap = str hh_map {CUSTOMERS} ;
        %end ;
        %else %do ;
          %let hhmap = num hh_map {CUSTOMERS} ;
        %end ;
        line = "&hhmap. ;" ;
        output ;
      %end ;
      %if (&active_contact_history_flg. = 1) %then %do ;
        line = "num HIST_CONTACTS {HIST_CONTACTS_SET} ;" ;
        output ;
      %end ;
      line = "  num c, o ;" ;
      output ;
    end ;
    
    keep line ;
  run ;

  %ConvertFormulas(inds=measures,outds=_calc_measures_,source_txt_var=formula_txt,dest_txt_var=new_formula_txt) ;

  proc sort data=_calc_measures_ (where=(trim(left(formula_txt)) > "")) ; by CalcOrder ; run ;

  data var_format ;
    set measures (rename=(array_ref=label measure=start)) ;
    length fmtname $7. type $1.;
    retain fmtname "optvar" ;
    type = "C" ;
  run ;

  proc format CNTLIN=var_format ;
  run ; 

  data CalcMeasures ;
    set _calc_measures_ end=lastrec;
    length line $2000 ;
    if _N_ = 1 then do ;
      line = " " ;
      output ;
      line = "  ** Create calculated measures **;" ;
      output ;
    line = "  ** TIP: Comment out all calc measures that are not needed to save CPU and RAM**;" ;
      output ;
    end ;
    if index(array_ref,"[i,j]") > 0 then 
      line = "for {i in CUSTOMERS, j in OFFERS} " || trim(left(array_ref)) || " = " || trim(left(new_formula_txt)) || " ;" ;
    else if index(array_ref,"[i]") > 0 then 
      line = "for {i in CUSTOMERS} " || trim(left(array_ref)) || " = " || trim(left(new_formula_txt)) || " ;" ;
    else if index(array_ref,"[j]") > 0 then 
      line = "for {j in OFFERS} " || trim(left(array_ref)) || " = " || trim(left(new_formula_txt)) || " ;" ;
    else abort ;
    
    output ;
    keep line ;
  run ;

  proc sql noprint ;
    select distinct measure into: CustAttr separated by " " from measures 
    where measure_type_cd in ("CUSTNUM" "CUSTCHAR") AND calcOrder = . ;
  quit ;

  data constraints ;
    set &batch_MO_lib..constraints (where=(active_flg=1 AND upcase(constraint_type_cd) NE "REPORT ONLY")) end=lastrec ;
    length new_filter $2048. ;
    retain has_hh_constraint1 0 ;
    communication_filter_txt = trim(left(communication_filter_txt)) ;
    customer_filter_txt = trim(left(customer_filter_txt)) ;
    numeric_measure = upcase(numeric_measure) ;

    ** Convert formula to uppercase except for text literals **;
    array filters (*) communication_filter_txt customer_filter_txt ;
    do i = 1 to dim(filters) ;
      ** Convert formula to uppercase except for text literals **;
      write_upcase = 1 ;
      new_filter = "" ;
      do _pos_ = 1 to length(filters(i)) ;
        _chr_ = substr(filters(i),_pos_,1) ;
        if _chr_ in ("'" '"') then write_upcase = NOT write_upcase ;
        if write_upcase then substr(new_filter,_pos_,1) = upcase(_chr_) ;
        else substr(new_filter,_pos_,1) = _chr_ ;
      end ;
      filters(i) = trim(left(new_filter)) ;
    end ;

    ** optmodel IN clause needs { } instead of ( ) **;
    ** this code will fail if there are text literals in IN clause with () in them**;
    do i = 1 to dim(filters) ;
      substr_start_pos = 1 ;
      _pos_ = index(substr(filters(i),substr_start_pos)," IN ") ;
      str_length = length(filters(i)) ;
      do while (_pos_ > 0) ;
        _pos_start_ = _pos_ + 3 + index(substr(filters(i),_pos_+4),"(") ;
        _pos_end_ = _pos_start_ + index(substr(filters(i),_pos_start_+1),")") ;
        substr(filters(i),_pos_start_,1) = "{" ;
        substr(filters(i),_pos_end_,1) = "}" ;
        substr_start_pos = _pos_end_ + 1 ;
        if substr_start_pos <= str_length then _pos_ = index(substr(filters(i),substr_start_pos)," IN ") ;
        else _pos_ = 0 ;
      end ;
    end ;

    if (constraint_scope_cd = "HOUSEHOLD") then has_hh_constraint1 = 1 ;
    if lastrec then call symput("has_hh_constraint1",trim(left(put(has_hh_constraint1,8.)))) ;

    drop i write_upcase _pos_ _chr_ new_filter substr_start_pos _pos_ str_length _pos_start_ 
         _pos_end_ has_hh_constraint1 ;
  run ;

  %add_spaces(inds=constraints,source_txt_var=communication_filter_txt) ; 
  %add_spaces(inds=constraints,source_txt_var=customer_filter_txt) ; 

  %ConvertFormulas(inds=constraints,source_txt_var=communication_filter_txt) ;
  %ConvertFormulas(inds=constraints,source_txt_var=customer_filter_txt) ;

  data ConstraintFilterFormulas (keep=line) ;
    set constraints end=lastrec ;
    length line $2000. comm_filter_set_name cust_filter_set_name offer_set customer_set $40. ;

    if _N_ = 1 then do ;
      line = " " ;
      output ;
      line = "  ** Aggregate Constraints and set Definitions **;" ;
      output ConstraintFilterFormulas ;
    end ;

    if trim(left(communication_filter_txt)) > "" then do ;
      comm_filter_set_name = "Const" || trim(left(put(_N_,8.))) || "_Offers" ;
      line = "set " || trim(left(comm_filter_set_name)) || " = {j in &OFFERS_W_CONS_Set.: " || trim(left(communication_filter_txt)) || "} ;" ;
      output ConstraintFilterFormulas ;
    end ;

    if trim(left(customer_filter_txt)) > "" then do ;
      cust_filter_set_name = "Const" || trim(left(put(_N_,8.))) || "_Cust" ;
      line = "set " || trim(left(cust_filter_set_name)) || " = {i in CUSTOMERS: " || trim(left(customer_filter_txt)) || "} ;" ;
      output ConstraintFilterFormulas ;
    end ;

    ** possible to end up with duplicate constr ids **;
    constraint_id = "_" || trim(left(constraint_id)) ;
    line = "con " || translate(trim(left(constraint_id)),"_____"," $.-:") ;

    if comm_filter_set_name > "" then offer_set = comm_filter_set_name ;
      else offer_set = "&OFFERS_W_CONS_Set." ;
    if cust_filter_set_name > "" then Customer_set = cust_filter_set_name ;
      else Customer_set = "CUSTOMERS" ;
   
    if constraint_computation_cd = "NUM_OFFERS" then do ;
      if constraint_scope_cd = "AGGREGATE" then do ;
        line = trim(left(line)) || " : sum{i in " || trim(left(Customer_set)) || ",j in " || trim(left(offer_set)) || 
               "} MakeOffer[i,j]" || " " || trim(left(constraint_operator)) || " " || trim(left(put(constraint_rhs,32.10))) || 
               " * " || trim(left(put(&sample_ratio.,32.10))) || " ;" ;
      end ;
      else if constraint_scope_cd = "CUSTOMER" then do ;
        line = trim(left(line)) || "{i in " || trim(left(Customer_set)) || "} : sum{j in " || trim(left(offer_set)) || 
               "} MakeOffer[i,j] " || trim(left(constraint_operator)) || " " || trim(left(put(constraint_rhs,32.10))) || ";" ;
      end ;
      else if constraint_scope_cd = "HOUSEHOLD" then do ;
        line = trim(left(line)) || "{h in HOUSEHOLDS} : sum{i in " || trim(left(Customer_set)) || 
               ",j in " || trim(left(offer_set)) || " : hh_map[i]=h} MakeOffer[i,j] " || trim(left(constraint_operator)) || 
               " " || trim(left(put(constraint_rhs,32.10))) || ";" ;
      end ;
    end ;
    else if constraint_computation_cd = "SUM" then do ;
      if constraint_scope_cd = "AGGREGATE" then do ;
        line = trim(left(line)) || " : sum{i in " || trim(left(Customer_set)) || ",j in " || trim(left(offer_set)) || 
               ": " || put(numeric_measure,$optvar.) || " NE .} MakeOffer[i,j] * " || put(numeric_measure,$optvar.) || " " || 
               trim(left(constraint_operator)) || 
               " " || trim(left(put(constraint_rhs,32.10))) || " * " || trim(left(put(&sample_ratio.,32.10))) || " ;" ;
      end ;
      else if constraint_scope_cd = "CUSTOMER" then do ;
        line = trim(left(line)) || "{i in " || trim(left(Customer_set)) || "} : sum{j in " || trim(left(offer_set)) || 
               "} MakeOffer[i,j] * " || put(numeric_measure,$optvar.) || " " || trim(left(constraint_operator)) || 
               " " || trim(left(put(constraint_rhs,32.10))) || ";" ;
      end ;
      else if constraint_scope_cd = "HOUSEHOLD" then do ;
        line = trim(left(line)) || "{h in HOUSEHOLDS} : sum{i in " || trim(left(Customer_set)) || ",j in " || trim(left(offer_set)) ||
               " : hh_map[i]=h} MakeOffer[i,j] * " || put(numeric_measure,$optvar.) || " " || trim(left(constraint_operator)) || 
               " " || trim(left(put(constraint_rhs,32.10))) || ";" ;
      end ;
    end ;
    else if constraint_computation_cd = "AVERAGE" then do ;  
      if constraint_scope_cd = "AGGREGATE" then do ;
        line = trim(left(line)) || " : sum{i in " || trim(left(Customer_set)) || ",j in " || trim(left(offer_set)) || 
               ": " || put(numeric_measure,$optvar.) || " NE .} MakeOffer[i,j] * " || put(numeric_measure,$optvar.) || " " || 
               trim(left(constraint_operator)) || " " || trim(left(put(constraint_rhs,32.10))) || 
               " * sum{i in " || trim(left(Customer_set)) || 
               ",j in " || trim(left(offer_set)) || ": " || put(numeric_measure,$optvar.) || " NE .} MakeOffer[i,j] ;" ;
      end ;
      else if constraint_scope_cd = "CUSTOMER" then do ;
        line = trim(left(line)) || "{i in " || trim(left(Customer_set)) || "} : sum{j in " || trim(left(offer_set)) || 
               "} MakeOffer[i,j] * " || put(numeric_measure,$optvar.) || " " || trim(left(constraint_operator)) || 
               " " || trim(left(put(constraint_rhs,32.10))) || " * sum{j in " || trim(left(offer_set)) || "} MakeOffer[i,j] ;" ;
      end ;
      else if constraint_scope_cd = "HOUSEHOLD" then do ;
        line = trim(left(line)) || "{h in HOUSEHOLDS} : sum{i in " || trim(left(Customer_set)) || ",j in " || 
               trim(left(offer_set)) || " : hh_map[i]=h} MakeOffer[i,j] * " || put(numeric_measure,$optvar.) || " " 
               || trim(left(constraint_operator)) || " " || trim(left(put(constraint_rhs,32.10))) || 
               " * sum{i in " || trim(left(Customer_set)) || ",j in " || trim(left(offer_set)) || 
               " : hh_map[i]=h} MakeOffer[i,j] ;" ;
      end ;
    end ;
    else if constraint_computation_cd = "RATIO" then do ;
      if constraint_scope_cd = "AGGREGATE" then do ;
        line = trim(left(line)) || " : sum{i in " || trim(left(Customer_set)) || ",j in " || trim(left(offer_set)) || 
               ": " || put(scan(numeric_measure,1," "),$optvar.) || " NE . AND " || put(scan(numeric_measure,2," "),$optvar.) ||
               " NE .} MakeOffer[i,j] * " || put(scan(numeric_measure,1," "),$optvar.) || " " || trim(left(constraint_operator)) || 
               " " || trim(left(put(constraint_rhs,32.10))) || " * sum{i in " || trim(left(Customer_set)) || ",j in " || 
               trim(left(offer_set)) || ": " || put(scan(numeric_measure,1," "),$optvar.) || " NE . AND " || 
               put(scan(numeric_measure,2," "),$optvar.) || " NE .} MakeOffer[i,j] * " || put(scan(numeric_measure,2," "),$optvar.) || ";" ;
      end ;
      else if constraint_scope_cd = "CUSTOMER" then do ;
        line = trim(left(line)) || "{i in " || trim(left(Customer_set)) || "} : sum{j in " || trim(left(offer_set)) || 
               "} MakeOffer[i,j] * " || put(scan(numeric_measure,1," "),$optvar.) || " " || trim(left(constraint_operator)) || 
               " " || trim(left(put(constraint_rhs,32.10))) || " * sum{j in " || trim(left(offer_set)) || "} MakeOffer[i,j] * " || 
               put(scan(numeric_measure,2," "),$optvar.) || ";" ;
      end ;
      else if constraint_scope_cd = "HOUSEHOLD" then do ;
        line = trim(left(line)) || "{h in HOUSEHOLDS} : sum{i in " || trim(left(Customer_set)) || ",j in " || 
               trim(left(offer_set)) || " : hh_map[i]=h} MakeOffer[i,j] * " || put(scan(numeric_measure,1," "),$optvar.) || 
               " " || trim(left(constraint_operator)) || " " || trim(left(put(constraint_rhs,32.10))) || 
               " * sum{i in " || trim(left(Customer_set)) || ",j in " || trim(left(offer_set)) || 
               " : hh_map[i]=h} MakeOffer[i,j] * " || put(scan(numeric_measure,2," "),$optvar.) || ";" ;
      end ;
    end ;
    else abort ;

    output ConstraintFilterFormulas ;
  if not lastrec then do ;
    line = "" ;
    output ;
  end ;
    drop offer_set customer_set ;
  run ;

  data contact_policies ;
    set &batch_MO_lib..contact_policies (where=(active_flg=1)) end=lastrec ;
    length new_filter $2048. ;
    retain has_hh_constraint2 0 ;
    array filters (*) communication_filter_txt customer_filter_txt blocking_comm_filter_a_txt blocking_comm_filter_b_txt ;
    do i = 1 to dim(filters) ;
      ** Convert formula to uppercase except for text literals **;
      write_upcase = 1 ;
      new_filter = "" ;
      do _pos_ = 1 to length(filters(i)) ;
        _chr_ = substr(filters(i),_pos_,1) ;
        if _chr_ in ("'" '"') then write_upcase = NOT write_upcase ;
        if write_upcase then substr(new_filter,_pos_,1) = upcase(_chr_) ;
        else substr(new_filter,_pos_,1) = _chr_ ;
      end ;
      filters(i) = trim(left(new_filter)) ;
    end ;

    ** optmodel IN clause needs { } instead of ( ) **;
    ** this code will fail if there are text literals in IN clause with () in them**;
    do i = 1 to dim(filters) ;
      substr_start_pos = 1 ;
      _pos_ = index(substr(filters(i),substr_start_pos)," IN ") ;
      str_length = length(filters(i)) ;
      do while (_pos_ > 0) ;
        _pos_start_ = _pos_ + 3 + index(substr(filters(i),_pos_+4),"(") ;
        _pos_end_ = _pos_start_ + index(substr(filters(i),_pos_start_+1),")") ;
        substr(filters(i),_pos_start_,1) = "{" ;
        substr(filters(i),_pos_end_,1) = "}" ;
        substr_start_pos = _pos_end_ + 1 ;
        if substr_start_pos <= str_length then _pos_ = index(substr(filters(i),substr_start_pos)," IN ") ;
        else _pos_ = 0 ;
      end ;
    end ;

    if (target_level_cd = "HOUSEHOLD") then has_hh_constraint2 = 1 ;
    if lastrec then call symput("has_hh_constraint2",trim(left(put(has_hh_constraint2,8.)))) ;

    drop i write_upcase _pos_ _chr_ new_filter substr_start_pos _pos_ str_length _pos_start_ 
         _pos_end_ has_hh_constraint2 ;
  run ;

  %let has_hh_constraint = 0 ;
  %if ((&has_hh_constraint1. > 0) OR (&has_hh_constraint2. > 0)) %then 
    %let has_hh_constraint = 1 ;

  %add_spaces(inds=contact_policies,source_txt_var=communication_filter_txt) ; 
  %add_spaces(inds=contact_policies,source_txt_var=customer_filter_txt) ; 
  %add_spaces(inds=contact_policies,source_txt_var=blocking_comm_filter_a_txt) ; 
  %add_spaces(inds=contact_policies,source_txt_var=blocking_comm_filter_b_txt) ; 

  %ConvertFormulas(inds=contact_policies,source_txt_var=communication_filter_txt) ;
  %ConvertFormulas(inds=contact_policies,source_txt_var=customer_filter_txt) ;
  %ConvertFormulas(inds=contact_policies,source_txt_var=blocking_comm_filter_a_txt) ;
  %ConvertFormulas(inds=contact_policies,source_txt_var=blocking_comm_filter_b_txt) ;

  %if (&has_time_periods.) %then %do ;
    ** Explode out records when there are time periods with rolling lenghts **;
    data contact_policies ;
      set contact_policies ;

      if trim(left(start_time_period)) <= "" then start_time_period_no = &min_time_period_no. ;
        else start_time_period_no = max(&min_time_period_no.,input(start_time_period,timenum.)) ;
      if trim(left(end_time_period)) <= "" then end_time_period_no = &max_time_period_no. ;
        else end_time_period_no = min(&max_time_period_no.,input(end_time_period,timenum.)) ;

      if rolling_period_cnt ne . then rolling_period = rolling_period_cnt ;
      else if blocking_period_cnt ne . then rolling_period = blocking_period_cnt ;

      if rolling_period ne . then do ;
        iter = 0 ;
        start_no = 0  ;
        end_no = -1 ;
        do until (end_no = end_time_period_no) ;
          start_no = start_time_period_no + iter ;
          end_no = min(end_time_period_no,start_no + rolling_period - 1) ;
          output ;
          iter = iter + 1 ;
        end ;
      end ;
      else output ;
      *drop rolling_period iter ;
    run ;
  %end ;

  data contact_policy_Formulas (keep=line) ;
    set contact_policies end=lastrec ;
    length con_line line hist_str $2000. offer_set customer_set offer_setA offer_setB $40. ;
    %if (&active_contact_history_flg. = 1) %then %do ;
    length Hoffer_set offer_setB_Hist offer_setA_Hist $40. ;
  %end ;
    if _N_ = 1 then do ;
      line = " " ;
      con_line = " " ; 
      output ;
      line = "  ** Contact Policy Constraints:  **;" ;
      output ;
    end ;

    ** possible to end up with duplicate constr ids **;
    contact_policy_id = "_" || trim(left(substr(contact_policy_id,1,27))) || trim(left(put(_N_,z4.))) ;
    con_line = "con " || translate(trim(left(contact_policy_id)),"_____"," $.-:") ; 
   
    if (trim(left(blocking_comm_filter_a_txt)) <= "") AND (trim(left(communication_filter_txt)) > "" OR (end_no NE .) OR (start_no ne .)) then do ;
      if trim(left(communication_filter_txt)) > "" AND (end_no NE .) then 
        line = "set Policy" || trim(left(put(_N_,8.))) || "_Offers = {j in &OFFERS_W_POLICY_Set.: (" || trim(left(communication_filter_txt)) || 
               ") AND (time_period_order_no[j] >= " || trim(left(put(start_no,8.))) || ") AND (time_period_order_no[j] <= " || 
               trim(left(put(end_no,8.))) || ")} ;";
      else if trim(left(communication_filter_txt)) > "" then 
        line = "set Policy" || trim(left(put(_N_,8.))) || "_Offers = {j in &OFFERS_W_POLICY_Set.: (" || trim(left(communication_filter_txt)) || 
               ")} ;";
      else 
        line = "set Policy" || trim(left(put(_N_,8.))) || "_Offers = {j in &OFFERS_W_POLICY_Set.: (time_period_order_no[j] >= " || 
               trim(left(put(start_no,8.))) || ") AND (time_period_order_no[j] <= " || 
               trim(left(put(end_no,8.))) || ")} ;";
      offer_set = "Policy" || trim(left(put(_N_,8.))) || "_Offers" ;
      output ;
    end ;
    else offer_set = "&OFFERS_W_POLICY_Set." ;

    %if (&active_contact_history_flg. = 1) %then %do ;
      if (trim(left(blocking_comm_filter_a_txt)) <= "") AND (trim(left(communication_filter_txt)) > "" OR (end_no NE .) OR (start_no ne .)) then do ;
        if trim(left(communication_filter_txt)) > "" AND (end_no NE .) then 
          line = "set HPolcy" || trim(left(put(_N_,8.))) || "_Offers = {j in HIST_OFFERS: (" || trim(left(communication_filter_txt)) || 
                 ") AND (time_period_order_no[j] >= " || trim(left(put(start_no,8.))) || ") AND (time_period_order_no[j] <= " || 
                 trim(left(put(end_no,8.))) || ")} ;";
        else if trim(left(communication_filter_txt)) > "" then 
          line = "set HPolcy" || trim(left(put(_N_,8.))) || "_Offers = {j in HIST_OFFERS: (" || trim(left(communication_filter_txt)) || 
                 ")} ;";
        else 
          line = "set HPolcy" || trim(left(put(_N_,8.))) || "_Offers = {j in HIST_OFFERS: (time_period_order_no[j] >= " || 
                 trim(left(put(start_no,8.))) || ") AND (time_period_order_no[j] <= " || 
                 trim(left(put(end_no,8.))) || ")} ;";
        Hoffer_set = "HPolcy" || trim(left(put(_N_,8.))) || "_Offers" ;
        output ;
      end ;
      else Hoffer_set = "HIST_OFFERS" ; 
    %end ; 

    if trim(left(customer_filter_txt)) > "" then do ;
      line = "Set Policy" || trim(left(put(_N_,8.))) || "_Custs = {i in CUSTOMERS: (" || trim(left(customer_filter_txt)) || ")} ;";
      customer_set = "Policy" || trim(left(put(_N_,8.))) || "_Custs" ;
      output ;
    end ;
    else customer_set = "CUSTOMERS" ;

    %if (&active_contact_history_flg. = 1) %then %do ;
    if trim(left(Hoffer_set)) > "" then do ;
      if target_level_cd = "CUSTOMER" then
        hist_str = "+ sum{j in " || trim(left(Hoffer_set)) || "} HIST_CONTACTS[i,j] " ;
      else 
        hist_str = "+ sum{i in CUSTOMERS, j in " || trim(left(Hoffer_set)) || " : hh_map[i]=h} HIST_CONTACTS[i,j] " ;
    end ;
    else 
        hist_str = "" ; 
    %end ;
    %else %do ;
      hist_str = "" ;
    %end ;

    if trim(left(blocking_comm_filter_a_txt)) <= "" then do ;
      if target_level_cd = "CUSTOMER" then 
        con_line = trim(left(con_line)) || " {i in " || trim(left(customer_set)) || "}: sum{j in " || trim(left(offer_set)) || 
                   "} MakeOffer[i,j] " || trim(left(hist_str)) || " " || trim(left(contact_policy_operator)) || " " || 
                   trim(left(put(contact_limit_cnt,8.))) || " ;" ;
      else if target_level_cd = "HOUSEHOLD" then 
        con_line = trim(left(con_line)) || " {h in HOUSEHOLDS} : sum{i in " || trim(left(customer_set)) || ",j in " || 
                   trim(left(offer_set)) || " : hh_map[i]=h} MakeOffer[i,j] " || trim(left(hist_str)) || " " || 
                   trim(left(contact_policy_operator)) || " " || trim(left(put(contact_limit_cnt,8.))) || " ;" ;
      line = trim(left(con_line)) ;
      output ;
    end ;
    else do ;
      line = "set Block" || trim(left(put(_N_,8.))) || "_AOffers = {j in &OFFERS_W_POLICY_Set.: (" || trim(left(blocking_comm_filter_a_txt)) || 
             ") AND (time_period_order_no[j] >= " || trim(left(put(start_no,8.))) || ") AND (time_period_order_no[j] <= " || 
            trim(left(put(end_no,8.))) || ")} ;";
      offer_setA = "Block" || trim(left(put(_N_,8.))) || "_AOffers" ;
      output ;

      line = "set Block" || trim(left(put(_N_,8.))) || "_BOffers = {j in &OFFERS_W_POLICY_Set.: (" || trim(left(blocking_comm_filter_b_txt)) || 
             ") AND (time_period_order_no[j] >= " || trim(left(put(start_no,8.))) || ") AND (time_period_order_no[j] <= " || 
            trim(left(put(end_no,8.))) || ")} ;";
      offer_setB = "Block" || trim(left(put(_N_,8.))) || "_BOffers" ;
      output ;

      %if (&active_contact_history_flg. = 1) %then %do ;
        line = "set Block" || trim(left(put(_N_,8.))) || "_AOfferH = {j in HIST_OFFERS: (" || trim(left(blocking_comm_filter_a_txt)) || 
               ") AND (time_period_order_no[j] >= " || trim(left(put(start_no,8.))) || ") AND (time_period_order_no[j] <= " || 
              trim(left(put(end_no,8.))) || ")} ;";
        offer_setA_Hist = "Block" || trim(left(put(_N_,8.))) || "_AOfferH" ;
        output ;

        line = "set Block" || trim(left(put(_N_,8.))) || "_BOfferH = {j in HIST_OFFERS: (" || trim(left(blocking_comm_filter_b_txt)) || 
               ") AND (time_period_order_no[j] >= " || trim(left(put(start_no,8.))) || ") AND (time_period_order_no[j] <= " || 
              trim(left(put(end_no,8.))) || ")} ;";
        offer_setB_Hist = "Block" || trim(left(put(_N_,8.))) || "_BOfferH" ;
        output ;

        hist_str = "+ sum{l in " || trim(left(offer_setA_Hist)) || "} HIST_CONTACTS[i,l] + sum{m in " || 
                 trim(left(offer_setB_Hist)) || "} HIST_CONTACTS[i,m]" ;
      %end ;
      %else %do ;
        hist_str = "" ;
      %end ;
  
      if target_level_cd = "CUSTOMER" then 
        con_line = trim(left(con_line)) || " {i in " || trim(left(customer_set)) || ", j in " || trim(left(offer_setA)) || 
                   ", k in " || trim(left(offer_setB)) || "}: MakeOffer[i,j] + MakeOffer[i,k] " || trim(left(hist_str)) || 
                   " <= 1 ;" ;
      else if target_level_cd = "HOUSEHOLD" then 
        con_line = trim(left(con_line)) || " {h in HOUSEHOLDS, j in " || trim(left(offer_setA)) || 
                   ", k in " || trim(left(offer_setB)) || "}: sum{i in " || trim(left(customer_set)) || " : hh_map[i]=h} " || 
                   "MakeOffer[i,j] + MakeOffer[i,k] " || trim(left(hist_str)) || " <= 1 ;" ;
      line = trim(left(con_line)) ;
      output ;
      
    end ; 
  if not lastrec then do ;
    line = "" ;
    output ;
  end ;
  run ;

  data Suppression_Rules0 ;
    set &batch_MO_lib..Suppression_Rules (where=(active_flg=1)) ;
    array filters (*) communication_filter_txt customer_filter_txt ;
    length new_filter $2048. ;
    do i = 1 to dim(filters) ;
      ** Convert formula to uppercase except for text literals **;
      write_upcase = 1 ;
      new_filter = "" ;
      do _pos_ = 1 to length(filters(i)) ;
        _chr_ = substr(filters(i),_pos_,1) ;
        if _chr_ in ("'" '"') then write_upcase = NOT write_upcase ;
        if write_upcase then substr(new_filter,_pos_,1) = upcase(_chr_) ;
        else substr(new_filter,_pos_,1) = _chr_ ;
      end ;
      filters(i) = trim(left(new_filter)) ;
    end ;

    ** optmodel IN clause needs { } instead of ( ) **;
    ** this code will fail if there are text literals in IN clause with () in them**;
    do i = 1 to dim(filters) ;
      substr_start_pos = 1 ;
      _pos_ = index(substr(filters(i),substr_start_pos)," IN ") ;
      str_length = length(filters(i)) ;
      do while (_pos_ > 0) ;
        _pos_start_ = _pos_ + 3 + index(substr(filters(i),_pos_+4),"(") ;
        _pos_end_ = _pos_start_ + index(substr(filters(i),_pos_start_+1),")") ;
        substr(filters(i),_pos_start_,1) = "{" ;
        substr(filters(i),_pos_end_,1) = "}" ;
        substr_start_pos = _pos_end_ + 1 ;
        if substr_start_pos <= str_length then _pos_ = index(substr(filters(i),substr_start_pos)," IN ") ;
        else _pos_ = 0 ;
      end ;
    end ;

    drop i write_upcase _pos_ _chr_ new_filter substr_start_pos _pos_ str_length _pos_start_ _pos_end_ ;
  run ;

  %add_spaces(inds=Suppression_Rules0,source_txt_var=communication_filter_txt) ; 
  %add_spaces(inds=Suppression_Rules0,source_txt_var=customer_filter_txt) ; 

  %ConvertFormulas(inds=Suppression_Rules0,source_txt_var=communication_filter_txt) ;
  %ConvertFormulas(inds=Suppression_Rules0,source_txt_var=customer_filter_txt) ;

  data Suppression_Rules ;
    set Suppression_Rules0 ;
    length line $2000. _set_name_ $32. ;

    if _N_ = 1 then do ;
      line = " " ;
      output ;
      line = "  ** Suppression Rules and Set Definitions **;" ;
      output ;
    end ;

  _set_name_ = "Supp" || trim(left(put(_N_,8.))) || "_Set" ;

  ** Note that for Suppression rules Customer filters there can be customer-comm level measures **;
    if (trim(left(customer_filter_txt)) > "" AND trim(left(communication_filter_txt)) > "") then do ;
      line = "set " || trim(left(_set_name_)) || " = {i in CUSTOMERS, j in OFFERS: (" || trim(left(customer_filter_txt)) || 
             ") AND (" || trim(left(communication_filter_txt)) || ")} ;";
      output ;
  end ;
  else if (index(customer_filter_txt,"[i,j]") > 0) then do ;
    line = "set " || trim(left(_set_name_)) || " = {i in CUSTOMERS, j in OFFERS: (" || trim(left(customer_filter_txt)) || ")} ;";
      output ;
  end ;
  else if trim(left(customer_filter_txt)) > "" then do ;
    line = "set " || trim(left(_set_name_)) || " = {i in CUSTOMERS, j in OFFERS: " || trim(left(customer_filter_txt)) || "} ;";
      output ; 
  end ;
  else if trim(left(communication_filter_txt)) > "" then do ;
      line = "set " || trim(left(_set_name_)) || " = {i in CUSTOMERS, j in OFFERS: " || trim(left(communication_filter_txt)) || "} ;";
    output ;
  end ;
    
  line = "for {<i,j> in " || trim(left(_set_name_)) || "} Fix MakeOffer[i,j] = 0 ;" ;
    output  ;

    keep line ;
  run ;

  data _Control_Table_ ;
    length numeric_measure $32. ;
    set &MO_inds_lib..&MO_Control_Table_Name. ;
    numeric_measure = upcase(numeric_measure) ;
  run ;

  proc sort NODUPKEY data=_Control_Table_ (keep=numeric_measure) out=Control_CustComm_Vars ;
    by numeric_measure ;
  run ;

  proc sort NODUPKEY data=measures (where=(measure_type_cd="CUSTCOMM" AND formula_txt <= "")) out=_Batch_CustComm_Vars ;
    by measure  ;
  run ;

  data _null_ ;
    merge Control_CustComm_Vars (in=incontrol)
          _Batch_CustComm_Vars  (in=inbatch rename=(measure=numeric_measure))
    ;
    by numeric_measure ;
    if not (first.numeric_measure and last.numeric_measure) then abort ;
    if inbatch and not incontrol then abort ;
  run ;

  data _Control_Table_ ;
    set _Control_Table_ ;
    set Comms (keep=campaign_cd communication_cd offer_id) key=campcomm / unique ;
  run ;

  proc sort data=_Control_Table_ ; by numeric_measure offer_id ; run ;

  data optmodelReadData (keep=line) 
       _Control_Table_ ;
    set _Control_Table_ end=lastrec ;
    by numeric_measure offer_id ;
    length new_var_name $32. line $2000. ;
    if _N_ = 1 then do ;
      line = "  ** read cust and cust-comm vars from customer table into arrays **;" ;
      output optmodelReadData;
      line = "  ** TIP: Only read in needed data fields to reduce CPU and RAM use**;" ;
      output optmodelReadData;
    end ;
    retain number_vars 0 ;
    if first.numeric_measure then do ;
      number_vars = number_vars + 1 ;
    end ;
    new_var_name = "_XXYQ" || trim(left(put(number_vars,8.))) || "_" || trim(left(put(offer_id,8.)));
    if _N_ = 1 then do ;
      line = "read data &Outlib..&CustomerDataOutView. into CUSTOMERS=[&customer_id.] " ;
      output optmodelReadData ;
    end ;
    if last.numeric_measure then do ;
      ** write out optmodel code for this var **;
      line = "{j in OFFERS} <" || trim(left(numeric_measure)) || "[&customer_id.,j]=col('" || "_XXYQ" || 
             trim(left(put(number_vars,8.))) || "_'||j)>" ;
      output optmodelReadData;
    end ;
    if lastrec then do ;
      %if "&household_id." ne "" %then %do ;
        line = "&CustAttr. hh_map=&household_id. ;" ;
      %end ;
      %else %do ;
        line = "&CustAttr. ;" ;
      %end ;
      output optmodelReadData;
      %if (&active_contact_history_flg. = 1) %then %do ;
        line = "" ;
        output optmodelReadData ;
        line = "** Read Historical Contacts for time based contact policies **;" ;
        output optmodelReadData ;
        line = "read data &Outlib..&ContactHistoryOutView. into HIST_CONTACTS_SET=[&customer_id. offer_id] HIST_CONTACTS=offer ;" ;
        output optmodelReadData ;
      %end ;
    end ;
    output _Control_Table_ ;
  run ;

  %if (&active_contact_history_flg. = 1) %then %do ;

    proc datasets library=&Outlib. nolist ;
      delete _ContactHistory0_ _temp_ &ContactHistoryOutView. / memtype=view ;
    quit ;

    data &Outlib.._ContactHistory0_ / view=&Outlib.._ContactHistory0_ ;
      set &MO_inds_lib..&contact_history_table_name. ;
      length offer_id offer 8. ;
      offer_id = input(put(UNIQUE_COMM_MAPPING_ID,HC_Map.),8.) ;
      Offer = 1 ;
      drop UNIQUE_COMM_MAPPING_ID ;
    run ;

    proc sql noprint ;
      create view &Outlib.._temp_ as select &customer_id., offer_id  
        from &MO_inds_lib..&MO_Customer_Table_Name. (keep=&customer_id.),
        &Outlib..&OfferDataOutDs. (keep=offer_id unique_comm_mapping_id where=(unique_comm_mapping_id NE .)) ;
      create view &Outlib..&ContactHistoryOutView. as select A.&customer_id., A.offer_id, sum(0,B.offer) as offer from _temp_ as A left join _ContactHistory0_ as B
        on A.&customer_id. = B.&customer_id. AND A.offer_id = B.offer_id ;
    quit ;

    data _null_ ;
      file &mkdatafilename. LRECL=1000 MOD ;
      length line $1000. ;
      line ="data &Outlib.._ContactHistory0_ / view=&Outlib.._ContactHistory0_ ;" ;
      put line ;
      line = "set &MO_inds_lib..&contact_history_table_name. ;" ;
      put @3 line ;
      line = "length offer_id offer 8. ;" ;
      put @3 line ;
      line = "offer_id = input(put(UNIQUE_COMM_MAPPING_ID,HC_Map.),8.) ;" ;
      put @3 line ;
      line = "Offer = 1 ;" ;
      put @3 line ;
      line = "drop UNIQUE_COMM_MAPPING_ID ;" ;
      put @3 line ;
      line = "run ;" ;
      put line ;
      line = " " ;
      put line ;
      line = "proc sql noprint ;" ;
      put line ;
      line = "create view &Outlib.._temp_ as select &customer_id., offer_id  " ;
      put @3 line ;
      line = "from &MO_inds_lib..&MO_Customer_Table_Name. (keep=&customer_id.)," ;
      put @5 line ;
      line = "&Outlib..&OfferDataOutDs. (keep=offer_id unique_comm_mapping_id where=(unique_comm_mapping_id NE .)) ;" ;
      put @5 line ;
      line = "create view &Outlib..&ContactHistoryOutView. as select A.&customer_id., A.offer_id, sum(0,B.offer) as offer from _temp_ as A left join _ContactHistory0_ as B" ;
      put @3 line ;
      line = "  on A.&customer_id. = B.&customer_id. AND A.offer_id = B.offer_id ;" ;
      put @5 line ;
      line = "quit ;" ;
      put line ;
      line = " " ;
      put line ;
    run ;
 
  %end ;

  filename tmp "temp.sas" ;
  data _null_ ;
    set _Control_Table_ ;
    file tmp lrecl=1000 ;
    *if _N_ = 1 then put "rename " ;
    *put column_nm " = " new_var_name ;
    put new_var_name " = " column_nm " ; " ;
  run ;

  proc datasets library=&Outlib. nolist ;
    delete &CustomerDataOutView. / memtype=view ;
  quit ;

  ** If running on a sample and have household level constraints or contact policies **;
  ** MO samples households and then uses all customers in hh sample **;
  %if ((&has_hh_constraint. = 1) AND (&sample_ratio. < 1)) %then %do ;
    proc sort NODUPKEY data=&MO_inds_lib..&MO_Customer_Table_Name. (keep=&household_id.) out=_hhlist_ ;  
      by &household_id. ;
    run ;

    data _hh_sample_ ;
      set _hhlist_ ;
      __x = ranuni(&sample_seed.) ;
      if __x <= &sample_ratio. ; 
      drop __x ;
    run ;

    proc sql noprint ;
      create view _source_data_ as select cust.* from _hh_sample_ as hh left join 
      &MO_inds_lib..&MO_Customer_Table_Name. as cust 
      on hh.&household_id. = cust.&household_id. ;  
    quit ;

    data _null_ ;
      file &mkdatafilename. LRECL=1000 MOD ;
      length line $1000. ;
      line = "proc sort NODUPKEY data=&MO_inds_lib..&MO_Customer_Table_Name. (keep=&household_id.) out=_hhlist_ ;  " ;
      put line ;
      line = "by &household_id. ;" ;
      put @3 line ;
      line = "run ;" ;
      put line ;
      line = "  " ;
      put line ;
      line = "data _hh_sample_ ;" ;
      put line ;
      line = "set _hhlist_ ;" ;
      put @3 line ;
      line = "  __x = ranuni(&sample_seed.) ;" ;
      put @3 line ;
      line = "  if __x <= &sample_ratio. ; " ;
      put @3 line ;
      line = "  drop __x ;" ;
      put @3 line ;
      line = "run ;" ;
      put line ;
      line = "  " ;
      put line ;
      line = "proc sql noprint ;" ;
      put line ;
      line ="  create view _source_data_ as select cust.* from _hh_sample_ as hh left join " ;
      put @3 line ;
      line = "  &MO_inds_lib..&MO_Customer_Table_Name. as cust " ;
      put @3 line ;
      line = " on hh.&household_id. = cust.&household_id. ;" ;
      put @3 line ;
      line = "quit ;" ;
      put line ;
      line = "  " ;
      put line ;
    run ;

    %let cust_data_source = _source_data_ ;
  %end ;
  %else %do ;
    %let cust_data_source = &MO_inds_lib..&MO_Customer_Table_Name. ;
  %end ;

  data &Outlib..&CustomerDataOutView. / view=&Outlib..&CustomerDataOutView. ;
    set &cust_data_source. (obs=&CustObs.) ;  
    %if ("%upcase(&cust_data_source.)" NE "_SOURCE_DATA_") %then %do ;
      __x = ranuni(&sample_seed.) ;
      if __x <= &sample_ratio. ; 
      drop __x ;
    %end ;
    %include tmp / source2 ;;
  run ;

  data _null_ ;
    file &mkdatafilename. LRECL=1000 MOD ;
    length line $1000. ;
    line = "data &Outlib..&CustomerDataOutView. / view=&Outlib..&CustomerDataOutView. ;" ;
    put line ;
    line = "set &cust_data_source. (obs=&CustObs.) ;  " ;
    put @3 line ;
    %if ("%upcase(&cust_data_source.)" NE "_SOURCE_DATA_") %then %do ;
      line = "__x = ranuni(&sample_seed.) ;" ;
      put @3 line ;
      line = "if __x <= &sample_ratio. ; " ;
      put @3 line ;
      line = "drop __x ;" ;
      put @3 line ;
    %end ;
  run ;

  data _null_ ;
    infile tmp LRECL=1000 end=lastrec ;
    file &mkdatafilename. LRECL=1000 MOD ;
    input ;
    put @3 _infile_ ;
    if lastrec then do ;
      put "run ;" ;
      put " " ;
    end ;
  run ;

  ** Create the Agent constraints if they exist and are active **;
  %if "%trim(&active_agent_constraints_flg.)" = "1" %then %do ;

    proc sort data=&batch_MO_lib..time_periods out=time_periods ;
      by time_period ;
    run ;

    proc sort data=&batch_MO_lib..agent_Capacity out=Agent_Capacity ;
      by time_period ;
    run ;

    data Agent_Capacity ;
      merge Agent_Capacity (in=inagt)
            time_periods   (in=intime)
      ;
      by time_period ;
      if inagt ;
      if not intime then abort ;
    run ;

    proc sort data=Agent_Capacity ;
      by &agent_keyword. order_no ;
    run ;

    data agentConstraints ;
      set Agent_Capacity ;
      by &agent_keyword. order_no ;
      length line $2000. Name_Tag $32. ;
      retain _agent_id 0 ;
      if _N_ = 1 then do ;
        line = " " ;
        output ;
        line = "  ** Agent Capacity Constraints **;" ;
        output ;
      end ;
      if first.&agent_keyword. then _agent_id = _agent_id + 1 ;
      Name_Tag = "AgentCap_Agt" || trim(left(put(_agent_id,z5.))) || "_time" || trim(left(put(order_no,z5.))) ;

      line = "Set " || trim(left(Name_Tag)) || "_Cust = {i in CUSTOMERS: (&agent_keyword.[i] = '" || 
             trim(left(&agent_keyword.)) || "')} ;" ;
      output ;
      line = "Set " || trim(left(Name_Tag)) || "_Comm = {j in OFFERS: (CHANNEL_CD[j] = '" || "&agent_keyword." || 
             "') AND (TIME_PERIOD[j] = '" || trim(left(time_period)) || "')} ;" ;
      output ;
      line = "con " || trim(left(Name_Tag)) || "_Con : sum{i in " || trim(left(Name_Tag)) || "_Cust, " || 
           "j in " || trim(left(Name_Tag)) || "_Comm} MakeOffer[i,j] * UNIT_USAGE[j] <= " || trim(left(put(capacity,8.))) || 
           " * " || trim(left(put(&sample_ratio.,32.10))) || " ;" ;
      output ;
      line = "";
      output ;
    run ;
  %end ;

  %if (%upcase(%vartype(&MO_inds_lib..&MO_Customer_Table_Name.,&customer_id.)) = C) %then %do ;
    %let custdecl = <String> CUSTOMERS ;
  %end ;
  %else %do ;
    %let custdecl = CUSTOMERS;
  %end ;

  data optmodel_start ;
    length line $10000. ;
    line = "proc optmodel;" ;
    output ;
    line = "** This code was generated by the Convert_MO2OptModel Macro **;" ;
    output ;
    line = "** TIP: unused vars, arrays and set declarations do not noticably hurt performance **;" ;
    output ;
    line = "** See below in comments for tips to make this code run faster **;" ;
    output ;
    line = " " ;
    output ;
    line = "  ** declare index sets **;" ;
    output ;
    line = "  set &custdecl. ;" ;
    output ;
    %if "&household_id." ne "" %then %do ;
      line = "  set &hhdecl. ;" ;
      output ;
    %end ;
    line = "  set OFFERS ;" ;
    output ;
    %if (&active_contact_history_flg. = 1) %then %do ;
      line = "  set HIST_OFFERS ;" ;
      output ;
      line = "  set FULL_OFFERS ;" ;
      output ;
      line = "  set <num,num> HIST_CONTACTS_SET ;" ;
      output ;
    %end ;
    line = " " ;
    output ;
    line = "  ** declare decision variables **;" ;
    output ;
    line = "  var MakeOffer {CUSTOMERS, OFFERS} binary;" ;
    output ;
  run ;

  data ReadCommVars ;
    length line $10000. ;
    line = " " ;
    output ;
    line = "  ** read data from communication table into arrays **;" ;
    output ;
    line = "  ** TIP: Only read in needed data fields to reduce CPU and RAM use**;" ;
    output ;
    %if (&active_contact_history_flg. = 1) %then %do ;
      line = "  read data &Outlib..&OfferDataOutDs. into FULL_OFFERS=[offer_id] &comm_char_var_list. &comm_num_var_list. ;" ;
      output ;
      line = "" ;
      output ;
      line = "  HIST_OFFERS = {j in FULL_OFFERS: UNIQUE_COMM_MAPPING_ID[j] NE .} ;" ;
      output ;
      line = "  OFFERS = {j in FULL_OFFERS: UNIQUE_COMM_MAPPING_ID[j] = .} ;" ;
      output ;
    %end ;
    %else %do ;
      line = "  read data &Outlib..&OfferDataOutDs. into OFFERS=[offer_id] &comm_char_var_list. &comm_num_var_list. ;" ;
      output ;
    %end ;
    line = "" ;
    output ;
    line = "  print &comm_char_var_list. &comm_num_var_list. ;" ;
    output ;
    line = "" ;
    output ;
    %if %length(&Comms_Excl_Const.) %then %do ;      
      line = "  Set OFFERS_W_CONS = OFFERS DIFF {&Comms_Excl_Const.} ;" ;
      output ;
    %end ;
    %if %length(&Comms_Excl_ConPoly.) %then %do ;
      line = "  Set OFFERS_W_POLICY = OFFERS DIFF {&Comms_Excl_ConPoly.} ;" ;
      output ;
    %end ;
    %if %length(&Mandatory_Comms.) %then %do ;
      line = "  Set MANDATORY_OFFERS = {&Mandatory_Comms.} ;" ;
      output ;
    %end ;
  run ;

  %if "&household_id." ne "" %then %do ;

    proc datasets library=&Outlib. nolist ;
      delete &HouseholdOutView. / memtype=view ;
    quit ;

    proc sql noprint ;
      create view &Outlib..&HouseholdOutView. as select distinct &household_id. from &Outlib..&CustomerDataOutView. (keep=&household_id.) ;
    quit ;

    data _null_ ;
      file &mkdatafilename. LRECL=1000 MOD ;
      length line $1000. ;
      line = "proc sql noprint ;" ;
      put line ;
      line = "create view &Outlib..&HouseholdOutView. as select distinct &household_id. from &Outlib..&CustomerDataOutView. (keep=&household_id.) ;" ;
      put @3 line ;
      line = "quit ;" ;
      put line ;
      line = " " ;
      output ;
    run ;

    data read_hhs ;
      length line $2000. ;
      line = " " ;
      output ;
      line = "  ** read unique household list from customer table into array **;" ;
      output ;
      line = "  ** TIP: To save CPU and RAM use comment out hh data read if hh data not needed **;" ;
      output ;
      line = "  read data &Outlib..&HouseholdOutView. into HOUSEHOLDS=[&household_id.] ;" ;
      output ;
    run ;
 
  %end ;

  proc sql noprint ;
    select array_ref into: SolutionVarList separated by " " from measures where outputvar = 1 ;
  quit ;

  %if "&household_id." ne "" %then %do ;
    %let SolutionVarList = %str(&SolutionVarList. &household_id.=hh_map[i]) ;
  %end ;

  data FixVarsMissObj ;
    length line $2000. ;
    line = " " ;
    output ;
    line = "** Fix solution variables to zero when objective measure is missing;" ;
    output ;
    line = "for {i in CUSTOMERS, j in OFFERS} if (&objective_measure.[i,j] = .) then Fix MakeOffer[i,j] = 0 ; " ;
    output ;
  run ;

  data MandatoryOffers ;
    length line $2000. ;
    line = " " ;
    output ;
    line = "** Fix solution variables to one for Mandatory Offers;" ;
    output ;
    line = "for {i in CUSTOMERS, j in MANDATORY_OFFERS: MakeOffer[i,j] NE 0} Fix MakeOffer[i,j] = 1 ; " ;
    output ;
  run ;

  data optModelEnd ;
    set &batch_MO_lib..SCENARIO_PARAMETERS ;
    length line $2000. ;
    line = "" ;
    output ;
    line = "  ** Define Optimization Objective **;" ;
    output ;
    line = trim(left(objective_operator)) || " Total_Obj = sum{i in CUSTOMERS, j in OFFERS: &objective_measure.[i,j] NE .} " || 
           trim(left(put(upcase(objective_numeric_measure),$optvar.))) || " * MakeOffer[i,j];" ;
    output ;
    line = "" ;
    output ;
    line = "  ** Execute solver **;" ;
    output ;
    line = "  ** TIP: MILP solver options found here: http://support.sas.com/documentation/cdl/en/ormpug/67517/HTML/default/viewer.htm#ormpug_milpsolver_syntax02.htm **;" ;
    output ;
    line = "solve with MILP / loglevel=2 ;" ;
    output ;
    line = " " ;
    output ;
    line = " ** Create Solution table **;" ;
    output ;
    line = "create data &Outlib..&SolutionOutds. from [&customer_id. Offer_id]= {i in CUSTOMERS, j in OFFERS: MakeOffer[i,j]>0.5} " || 
           "&SolutionVarList.  ;" ;
    output ;
    line = " " ;
    output ;
    line = "quit;" ;
    output ;
    keep line ;
  run ;
    
  data _null_ ;
    length line $10000 ;
    set optmodel_start
    varDeclarations
    ReadCommVars
    optmodelReadData
    %if "&household_id." ne "" %then %do ;
      read_hhs
    %end ;
    CalcMeasures
    /*FixVarsMissObj*/
    Suppression_Rules 
    %if %length(&Mandatory_Comms.) %then %do ;
      MandatoryOffers
    %end ;
    ConstraintFilterFormulas
    contact_policy_Formulas 
    %if "%trim(&active_agent_constraints_flg.)" = "1" %then %do ;
      agentConstraints (keep=line) 
    %end ; 
    optModelEnd end=lastrec
    ;
    file &outfilename. ;

    done = 0 ;
    i = 1 ;
    pos = 0 ;

    line = left(trim(line)) ;
    if _N_ = 1 OR lastrec then put line ;
    else do ;
      put +(2) @@ ;
      do until (done) ;
        word = scan(line,i," ") ;
        i = i + 1 ;
        if word > "" then do ;
          pos = pos + length(word) + 1;
          put word @@ ;
          if pos >= &OutFileRecLen. then do ;
            put ;
            pos = 0 ;
            put +(4) @@ ;
          end ;
        end ;
        else do ;
          done = 1 ;
          put ;
        end ;
      end ;
    end ;
  run ;   

%mend ;