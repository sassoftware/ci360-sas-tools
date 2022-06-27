/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : make_mo_input_data.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2020 January
/ LastModBy : Noah Powers
/ LastModDt : 1.20.2020
/ Purpose   : Automate the process of creating MO input datasets based on user specified
/             set of communications and campaigns.  User must also specify base customer 
/             table for communication scores to be created and added to the customer data.
/ FuncOutput: N/A
/ Usage     :
/ Notes     : Accomodate additional datasets such as time_periods, channels, agents, etc
/ 
/             A log-normal disctribution is used currently based on the mean and std provided 
/             but this will be expanded to additional distributions soon
/
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ XL_Comms_Path        The full filepath and filename of the Excel file that contains the
/                      input information about: campaigns on the 
/                      - Campaigns Sheet (required).
/                        campaign_cd is only required char column.
/                        Any other columns will be read into the campaign Table
/                      - score_prefixes Sheet (required)
/                        score_prefix is the only column expected and it is required
/                        There are 1 or more values expected here put in different rows
/                        For each value found, there are 4 corresponding columns expected
/                        in the Communications Sheet.  For example, if prob_resp was one of the
/                        values, then the following 4 columns (with sample values) would be 
/                        expected in the Communications Sheet:
/                         communication_cd  prob_resp_VAR      prob_resp_MEAN  prob_resp_STD  prob_resp_PCTMISS
/                         IntroRate          P_RespIntroRate      0.045          0.0720        0.00
/                         StandardRate      P_RespStandardRate  0.026          0.0468         0.00
/
/                      - Communications Sheet (required)
/                        campaign_cd AND communication_cd are required columns as well as all 4:
/                        <score_prefix>_VAR   <score_prefix>_MEAN <score_prefix>_STD  <score_prefix>_PCTMISS
/                        for each value of <score_prefix> found in the score_prefixes Sheet    
/
/ BaseCustTable        
/ CampTableName        Default=MO_Campaign
/ CommTableName        Default=MO_Communications
/ CtrlTableName        Default=MO_Control
/ CustTableName        Default=MO_Customers
/ Outlib               The name of the sas library where the list of files to read and schema files 
/                      will be saved.
/============================================================================================*/
%macro make_mo_input_data(XL_Comms_Path=,
                         BaseCustTable=,
                         CampTableName=MO_Campaign,
                         CommTableName=MO_Communications,
                         CtrlTableName=MO_Control,
                         CustTableName=MO_Customers,
                         Outlib       =) ;

  %local i prefixes measureList score_var_list score_mean_list score_std_list score_pmiss_list 
         NumPrefixes score_var measure ;

  libname _xl_ XLSX "&XL_Comms_Path." ;

  data prefixes0 ;
    set _xl_.score_prefixes ;
  run ;

  ** ADD: Validate that sheet exists with Score_Prefixes AND numeric_measure **;

  proc sql noprint ;
    select Score_Prefixes, 
           numeric_measure,
           MinVal,
           MaxVal
             into 
           :prefixes separated by " ", 
           :measureList separated by " ",
           :ScoreMinList separated by " ",
           :ScoreMaxList separated by " "
    from prefixes0 ;

    select Score_Prefixes, 
           Score_Prefixes, 
           Score_Prefixes, 
           Score_Prefixes, 
           Score_Prefixes, 
           Score_Prefixes into
     :score_var_list separated by "_VAR ",
     :score_mean_list separated by "_MEAN ",
     :score_std_list separated by "_STD ",
     :score_pmiss_list separated by "_PCTMISS ",
     :score_zeta_list separated by "_ZETA ",
     :score_sigma_list separated by "_SIGMA "
    from prefixes0 ;
  quit ;

  %let score_var_list = %trim(&score_var_list.)_VAR ;
  %let score_mean_list = %trim(&score_mean_list.)_MEAN ;
  %let score_std_list = %trim(&score_std_list.)_STD ;
  %let score_pmiss_list = %trim(&score_pmiss_list.)_PCTMISS ;
  %let score_zeta_list = %trim(&score_zeta_list.)_ZETA ;
  %let score_sigma_list = %trim(&score_sigma_list.)_SIGMA ;
  %let NumScores = %words(&measureList.) ;

  data communications0 ;
    set _xl_.communications ;
  run ;

  data &outlib..&CommTableName. (drop=&score_var_list. &score_mean_list. &score_std_list. &score_pmiss_list.) ;  
    set communications0 ;
  run ;

  ** ADD: Validate that sheet exists with campaign_cd communication_cd **;

  data &outlib..&CampTableName. ;
    set _xl_.Campaigns ;
  run ;

  data &outlib..&CtrlTableName. ;
    length column_nm numeric_measure $32. ;
    set communications0 (keep=campaign_cd communication_cd &score_var_list.) ;

    %do i = 1 %to &NumScores. ;
      %let score_var = %scan(&score_var_list.,&i.,%str( )) ;
      %let measure = %scan(&MeasureList.,&i.,%str( )) ;

      column_nm = trim(left(&score_var.)) ;
      numeric_measure = "&measure." ;
      output ;
    %end ;

    drop &score_var_list. ;
  run ;

  data communications ;
    set communications0 ;
    array nms   (*) &score_var_list. ;
    array means (*) &score_mean_list. ;
    array stds  (*) &score_std_list. ;
    array zeta  (*) &score_zeta_list. ;
    array sigma (*) &score_sigma_list. ;
   
    do i = 1 to dim(nms) ;
      if length(trim(left(nms(i)))) > 26 then abort ;
      zeta(i) = log(means(i)**2 / SQRT(stds(i)**2 + means(i)**2)) ;
      sigma(i) = SQRT(Log(1 + (stds(i)**2/means(i)**2))) ;
    end ;

    drop i ;
  run ;

  proc sql noprint ;
    %do k = 1 %to &NumScores. ;
      %let score_var = %scan(&score_var_list.,&k.,%str( )) ;
      %let zeta_var = %scan(&score_zeta_list.,&k.,%str( )) ;
      %let sigma_var = %scan(&score_sigma_list.,&k.,%str( )) ;
      %let pmiss_var = %scan(&score_pmiss_list.,&k.,%str( )) ;
      select &score_var. into: s&k._Varlist separated by " " from communications ;
      select &zeta_var. into: s&k._zetalist separated by " " from communications ;
      select &sigma_var. into: s&k._sigmalist separated by " " from communications ;
      select &pmiss_var. into: s&k._pmisslist separated by " " from communications ;
    %end ;  
  quit ;

  %let NumComms = %nobs(communications0) ;


  data &outlib..&CustTableName. ;
    set &BaseCustTable. ;

    %do k = 1 %to &NumScores. ;
      
      %let MinVal = %scan(&ScoreMinList.,&k.,%str( )) ;
      %let MaxVal = %scan(&ScoreMaxList.,&k.,%str( )) ;

      array scr&k.   (&NumComms.) &&s&k._Varlist. ;
      array s&k.zeta (&NumComms.) _TEMPORARY_ (&&s&k._zetalist.) ;
      array s&k.sigm (&NumComms.) _TEMPORARY_ (&&s&k._sigmalist.) ;
      array s&k.pmis (&NumComms.) _TEMPORARY_ (&&s&k._pmisslist.) ;

      do i = 1 to &NumComms. ;
        rand = rand("UNIFORM") ;
        normal = . ;
        lognorm = . ;
        do until (&MinVal. < lognorm < &MaxVal.) ;
          normal = rand("NORMAL") ;
          lognorm = exp(s&k.sigm(i) * normal + s&k.zeta(i)) ;
        end ;
       if rand < s&k.pmis(i) then scr&k.(i) = . ;
       else scr&k.(i) = lognorm ;
      end ;
      
    %end ;

    drop rand i normal lognorm ;
  run ;
 
  proc univariate data=&outlib..&CustTableName. ;
    var %do k = 1 %to &NumScores. ;  &&s&k._Varlist. %end;;
    histogram ;
  run ;

  %FINISH: 

%mend ;