/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : AppendMOSolution.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2019 August
/ Purpose   : Write a copy of the MO/EO solution dataset into the EO_Solutions table in the
/             InfoMap for CI360Direct.  And execute DM commands to update the metadata in the 
/             CI360 UI.
/ FuncOutput: N/A
/ Usage     :
/ Notes     :  
/   Should also validate that the solution variable data types (not just names) are as expected
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ OutDs            SAS library and dataset name (associated with the CI360 Direct tenant) where 
/                  the EO_Solutions will be saved
/ ExpIDVarName     The expected name of the ID variable in the solution datasets
/ ExpAgentVarName  The expected name of the Agent variable in the solution datasets
/ IDList           The pipe (|} delimited list of variable IDs in the Optimized Assignments folder
/============================================================================================*/
%macro AppendMOSolution(outds           =,
                        ExpIDVarName    =,
                        ExpAgentVarName =,
                        IDList          =);

  %local slash path InputDataName ScenarioName CustIDVarName AgentIDVarName done i ID NewIDList KeepList ;

  %put EO PostProcessing Beginning ;
  
  %if (&sysscp. = WIN) %then %do;
    %let slash = %str(\);
  %end;
  %else %do;
    %let slash = %str(/);
  %end ;

  *libname mooutput "D:\install\OptimizeAgents\146fce6007000138d64c33c1\SASEngageOptimize\scenarios\noah_powers\241647905\308243410\solution" ;
  %*let moError = 0 ;
  
  %if (&moError ne 0) %then %do;
    %put ERROR: AppendMOSolution post-process was not run because moError = &moError.;
  %end;
  %else %if (not %sysfunc(exist(mooutput.mo_solution))) %then %do;
    %put ERROR: mooutput.mo_solution does not exist.;
  %end;
  %else %do;

    proc sql noprint;
      select substr(path,1,length(path)-8) into :path from SAShelp.Vslib where upcase(libname)= "MOOUTPUT" ;
    quit;

    filename _tmp "%trim(&path.)eo_optimize.sas" ;

    data _null_ ;
      length InputDataName ScenarioName $32. ;
      retain InputDataName ScenarioName ;
      infile _tmp lrecl=32767 length=recLen end=lastrec; 
      input line_in $varying32767. recLen ;

      search_string = 'INPUT_DATA  =' ;
      start_string_pos = index(line_in,strip(search_string)) ;
      if start_string_pos > 0 then do ;
        start_pos = start_string_pos+length(search_string)+1 ;
        InputDataName = strip(substr(line_in,start_pos));
      end ; 

      search_string = 'SCENARIO =' ;
      start_string_pos = index(line_in,strip(search_string)) ;
      if start_string_pos > 0 then do ;
        start_pos = start_string_pos+length(search_string)+1 ;
        ScenarioName = strip(substr(line_in,start_pos));
      end ;

      if lastrec then do ;
        call symput("InputDataName",strip(InputDataName)) ;
        call symput("ScenarioName",strip(ScenarioName)) ;
      end ;
    run ;

    %put InputDataName=&InputDataName. ;
    %put ScenarioName=&ScenarioName. ;

    libname _tmp "%trim(&path.)batch&slash.tables" ;

    proc sql noprint ;
      select upcase(customer_id) into: CustIDVarName from _tmp.scenario_parameters ;
      select upcase(agent_keyword) into: AgentIDVarName from _tmp.scenario_parameters ;
    quit ;

    %let CustIDVarName = %trim(%left(&CustIDVarName.)) ;
    %let AgentIDVarName = %trim(%left(&AgentIDVarName.)) ;

    %if ("&CustIDVarName." NE "%upcase(&ExpIDVarName.)") %then %do ;
      %put %cmpres(W%upcase(arning): Customer ID variable (&CustIDVarName.) is different from expected value of &ExpIDVarName.. 
           EO solution will NOT be appended to Information Map);
      %goto FINISH ;
    %end ;
    %if %length(%trim(&AgentIDVarName.)) > 0 AND ("%trim(&AgentIDVarName.)" NE "%upcase(&ExpAgentVarName.)") %then %do ;
      %put %cmpres(W%upcase(arning): Agent ID variable (&AgentIDVarName.) is different from expected value of AGENT. 
           EO solution will NOT be appended to Information Map);
      %goto FINISH ;
    %end ;

    ** remove old optimization results **;
    proc sql noprint ; 
      delete * from &outds. where Scenario_Name = "&ScenarioName." ;
    quit ;

    proc sql noprint ;
      select name into: KeepList separated by " " from sashelp.vcolumn 
      where upcase(libname)="MOOUTPUT" AND upcase(memname)="MO_SOLUTION" AND
        upcase(name) in ("&CustIDVarName." "&AgentIDVarName." "CAMPAIGN_CD" "COMMUNICATION_CD" "TIME_PERIOD" "CHANNEL_CD") ;
    quit ;
      
    data soln / view=soln ;
      length Scenario_Name $32. ;
      set mooutput.mo_solution (keep=&KeepList.) ;
      Scenario_Name = "&ScenarioName." ;
    run ;

    proc append data=soln base=&outDs. FORCE ;
    run ;

    %let done = 0 ;
    %let i = 1;
    %let NewIDList = ;

    %do %while (NOT &done.) ;
      %let ID = %scan(&IDList.,&i.,%str(|)) ;
      %if not %length(&ID.) %then %let done = 1 ;
      %else %do ;
        %let NewIDLIst = &NewIDLIst. "root.&ID." ;
        %let i = %eval(&i. + 1) ;
      %end ;
    %end ;

    X dm gmd --character SQL --dataitems &NewIDList. ;

    %let done = 0 ;
    %let i = 1;

    %do %while (NOT &done.) ;
      %let ID = %scan(&IDList.,&i.,%str(|)) ;
      %if not %length(&ID.) %then %let done = 1 ;
      %else %do ;
        X dm smd --compatibleSubjectsOnly --id "root.&ID." ;
        %let i = %eval(&i. + 1) ;
      %end ;
    %end ;

    %FINISH: ;
    %mo_error_tracking;
  %end;
%mend;