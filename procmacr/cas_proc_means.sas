/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : CAS_Proc_Means.sas
/ Author    : Noah Powers
/ Created   : June 2021
/ Purpose   : This macro replicates proc means aggregation functions for input data in CAS.
/             
/ FuncOutput: NA
/ Usage     : 
/ Notes     : Perhaps there is a better way to re-imagine my process given new CAS features 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ inds           The name of the SAS input dataset to be aggregated
/ GroupVars      The list of variables on the by and class statements all together
/ InDsWhere      (optional) This is an optional where statement to be applied to the input
/                data prior to aggregation.
/ Vars           The list of numeric variables to calculate aggregate varialbes in AggTypeList
/                for each one.
/ AggTypeList    The list of aggregations to be created for each variable in the Vars list. The
/                list of possible values are: (dont include the quotes or commas in the macro parm)
/                "CSS", "CV", "KURTOSIS", "MAX", "MEAN", "MIN", "N", "NMISS", "PROBT", "SKEWNESS", 
/                "STD", "STDERR", "SUM", "T", "TSTAT", "USS", "VAR"
/ AggVarNames    The is the list of names to use for each combination of VAR x AGGTYPELIST provided
/                by the user.  For example if VAR=var1 var2 var3 and AGGTYPELIST=SUM MAX then this
/                list should have 6 = (3 x 2) values in the order first var and each agg type then 
/                next var each aggtype and so on.  Continuing our example this parameter would have
/                var1_sum var1_max var2_sum var2_max var3_sum var3_max
/ outds          The name of the output SAS dataset to create at the IDENTITYVAR level
/============================================================================================*/
%macro CAS_Proc_Means(inds           =,
                      GroupVars      =,
                      InDsWhere      =,
                      Vars           =,
                      AggTypeList    =,
                      AggVarNames    =,
                      outds          =
                      ) ;

  %local InDsLib InDsName OutDsLib OutDsName varlistq AggTypeListq GroupVarListq i var2transpose 
         LastGroupVar j NumVars var agg AggType join_method ;
  
  %let InDsLib  = %scan(&InDs.,1,%str(.)) ;
  %let InDsName = %scan(&InDs.,2,%str(.)) ;

  %if not %length(&InDsName.) %then %do ;
    %let InDsLib  = WORK ;
    %let InDsName = &InDs. ;
  %end ;        
  
  %if not %length(&outds.) %then %let outds = &inds. ;
  
  %let OutDsLib  = %scan(&OutDs.,1,%str(.)) ;
  %let OutDsName = %scan(&OutDs.,2,%str(.)) ;

  %if not %length(&OutDsName.) %then %do ;
    %let OutDsLib  = WORK ;
    %let OutDsName = &OutDs. ;
  %end ;        
  
  %let varlistq = %quotelst(&vars.,delim=%str(, )) ;
  %let AggTypeListq = %quotelst(&AggTypeList.,delim=%str(, )) ;
  
  %if %length(&GroupVars.) %then %do ;
    %let GroupVarListq = %quotelst(&GroupVars.,delim=%str(, )) ;    
    %let LastGroupVar = %scan(&GroupVars.,%words(&GroupVars.),%str( )) ;
  %end ;
  %let NumVars = %words(&vars.) ;
  
  %if not %length(&AggVarNames.) %then %do ;
  
    %do i = 1 %to %words(&AggTypeList.) ; 
      %let agg = %scan(&AggTypeList.,&i.,%str( )) ;
      %do j = 1 %to %words(&vars.) ; 
        %let var = %scan(&vars.,&j.,%str( )) ;  
        %let AggVarNames = &AggVarNames. &var._&agg. ; 
      %end ;
    %end ;
  
  %end ;
  
  %*put AggVarNames=&AggVarNames. ;
    
  proc cas ;
    simple.summary /
      inputs={&varlistq.},
      subSet={&AggTypeListq.},
      table={
            caslib="&InDsLib.",
            %if %length(&InDsWhere.) %then %do ;
              where="&InDsWhere.",
            %end ;
            %if %length(&GroupVars.) %then %do ;
              groupBy={&GroupVarListq.},
            %end ;
            name="&InDsName."
            },
      casout={caslib="&InDsLib.", name="AggData_", replace=True, replication=0} ;
  quit ;
  
  %do i = 1 %to %words(&AggTypeList.) ;
  
    %let AggType = %lowcase(%scan(&AggTypeList.,&i.,%str( ))) ;
    %if ("&AggType." = "n") %then %let Agg2transpose = _Nobs_ ;
    %else %let Agg2transpose = _&AggType._ ;
    
    proc cas ;
      transpose.transpose / 
        table={
              name="AggData_",
              caslib="&InDsLib.",
              groupBy={&GroupVarListq.}
              },
        id={'_Column_'},
        casOut={name="AggData_tr&i.", caslib="&InDsLib.", replace=true},
        transpose={"&Agg2transpose."} ;
    quit ;        
    
  %end ;
  
  %let join_method = merge ;
  %if %words(&AggTypeList.) = 1 %then %let join_method = set ;
  
  data &outds. ;
    &join_method. %do i = 1 %to %words(&AggTypeList.) ; 
       &InDsLib..AggData_tr&i. (in=intr&i. drop=_name_ 
                                rename=(%do j = 1 %to %words(&vars.) ; 
                                          %let var = %scan(&vars.,&j.,%str( )) ;
                                          &var.= %scan(&AggVarNames.,%eval(&j.+(&i.-1)*&NumVars.),%str( ))
                                        %end;))                                  
                  %end ;
    ;
    %if %length(&GroupVars.) %then %do ;
      by &GroupVars. ;
      if not (first.&LastGroupVar. and last.&LastGroupVar.) then abort ;
    %end ;
    if not (%do i = 1 %to %words(&AggTypeList.) ; intr&i AND %end; 1) then abort ;
  run ;

  
  proc datasets library=&InDsLib. nolist ;
    delete AggData_  %do i = 1 %to %words(&AggTypeList.) ; AggData_tr&i. %end;;
  quit ;
  
%mend ;