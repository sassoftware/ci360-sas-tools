/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Make_Product_Recommendations.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2021 July
/ LastModBy : Noah Powers
/ LastModDt : 07.07.2021
/ Purpose   : This macro scores an input identity id level dataset using the FACTORS output dataset from proc
/             factmac.  It produces scores for every product in the FACTORS dataset, sorts the products by
/             this score and returns the top NumRecsPerIdentity products per identity.  Where NumRecsPerIdentity
/             is a user supplied parameter.  The data to score should have one record per identity id.  It can contain 
/             both ids with product views and ids without product views. The records with identities in 
/             the FACTORS dataset will get scoring using the user supplied astore.  The other records will
/             get scored in a data step that mimics the astore calculations but where the identity factors 
/             and bais are ignored in the calculation.  The resulting psuedo-score can be used for an 
/             identity across products. 
/ FuncOutput: N/A
/ Usage     :
/ Notes     : 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name      Description
/ -------------------------------------------------------------------------------------
/ casSessionName       The name of the CAS session to use for proc fedsql
/ in2ScoreDs           The full name of the input dataset with identities that we want to score
/                      and generate prod recommendations 
/ IdentityProductExclDs (optional) If provided, this dataset is expected to contain unique
/                      combinations of identities and products that we want excluded for gettting 
/                      recommended such as products that have been viewed in the last 2 weeks
/ FactorsInds          The estimated coefficients dataset from proc factmac.  This includes the biases (e.g.
/                      intercepts) as well as the factor values 
/ astoreInds           The name for the astore scoring dataset to be used in scoring identity ids
/                      that are in the input to score dataset.
/ Identity_id_Var      default=identity_id The name of the column in the above input datasets
/                      that captures the customer ID or identity ID                   
/ product_id_var       The name of the column in the above input datasets that captures the 
/                      product ID.  Most commonly, this will be product_id or product_sku
/ DependentVar         The name of the column in the INABT_DS that will be predicted in proc
/                      factmac
/ Addl_Predictors      By default, the product_id and the identity_id are used as predictors
/                      but any additional columns provided in this parameter will be used
/                      as predictors in proc factmac.  These must be of type nomimal (not interval) 
/ NumRecsPerIdentity   The number of recommendations to include in the RECOMMENDATIONSOUTDS
/                      output datasset
/ RecommendationsOutds The full name of the output datasets to be created that captures the 
/                      the recommended products for both IDs in the ABT and also from IN2SCOREONLY_DS
/============================================================================================*/
%macro Make_Product_Recommendations(casSessionName       =,
                                    in2ScoreDs           =,
                                    IdentityProductExclDs=,
                                    FactorsInds          =,
                                    astoreInds           =, 
                                    Identity_id_Var      =identity_id,                        
                                    product_id_var       =,
                                    DependentVar         =,
                                    Addl_Predictors      =,
                                    NumRecsPerIdentity   =,
                                    RecommendationsOutds =                                                         
                                    ) ; 

  %local inLib inName best_nfactors best_learnstep best_maxiter NumFactors ScoreVars NumScoreVars OverAllBias 
         i j var Score v k;
  
  ** Extract Input library name **;
  %let inLib  = %scan(&in2ScoreDs.,1,%str(.)) ;
  %let inName = %scan(&in2ScoreDs.,2,%str(.)) ;

  %if not %length(&inName.) %then %do ;
    %let inLib  = WORK ;
    %let inName = &in2ScoreDs. ;
  %end ;        

  data &inLib..Identity_ids_modeled (keep=&Identity_id_Var.) ;
    set &FactorsInds. ;
    length &Identity_id_Var. %varlen(&in2ScoreDs.,&Identity_id_Var.);
    if variable = "&Identity_id_Var." ;
    &Identity_id_Var. = level ;
    format _character_ ;
  run ;

  data &inLib..Identities2Score (compress=YES partition=(&identity_id_var.)) ;
    merge &inLib..Identity_ids_modeled (in=inmodel)
          &in2ScoreDs.                 (in=in2score)
    ;
    by &Identity_id_Var. ;
    if not (first.&Identity_id_Var. and last.&Identity_id_Var.) then abort ;
    _modeled_identity_ = (inmodel) ;
    if in2score ;
    keep &Identity_id_Var. &Addl_Predictors. _modeled_identity_ ;
  run ;
  
  proc datasets library=&inLib. nolist;
    delete dentity_ids_modeled ;
  quit ;
  
  proc contents data=&FactorsInds. out=_factors_contents_ (keep=name type) noprint ;
  run ;
  
  proc sql noprint ;
    select count(*) into: NumFactors from _factors_contents_ where upcase(name) like "FACTOR%" ;
  quit ;
      
  %let ScoreVars = &product_id_var. &Addl_Predictors. ;
  %let NumScoreVars = %words(&ScoreVars.) ;

  proc sql noprint ;
    select bias into: OverAllBias from &FactorsInds.  where variable = "_GLOBAL_" ;
  quit ;
  
  data &inLib..Product_ids_modeled (keep=level _varlen_) ;
    set &FactorsInds. ;
    if variable = "&product_id_var." ;
    _varlen_ = length(level) ;
    format _character_ ;
  run ;
  
  %CAS_Proc_Means(inds           =&inLib..Product_ids_modeled,
                  GroupVars      =,
                  InDsWhere      =,
                  Vars           =_varlen_,
                  AggTypeList    =max,
                  AggVarNames    =maxlen,
                  outds          =&InLib..maxprodidlen
                  ) ;
                  
  proc sql noprint ;
    select maxlen into: product_id_var_len from &InLib..maxprodidlen ;
  quit ;
  
  data &inLib..Product_ids_modeled ;
    length &product_id_var. $&product_id_var_len. ;
    set &inLib..Product_ids_modeled ;
    &product_id_var. = strip(level) ;
    keep &product_id_var. ;
  run ;
                  
  data _allvars_ ;
    set &inLib..Identities2Score (obs=1) ;
    length &product_id_var. $&product_id_var_len. ;
  run ;
                  
  data %do i = 1 %to &NumScoreVars. ; 
         &inLib..Var&i.Lookup (compress=YES rename=(bias=Var&i.bias 
                                   %do j = 1 %to &NumFactors ;
                                     factor&j. = var&i.factor&j. 
                                   %end ;
                                   ) keep=bias factor: %scan(&ScoreVars.,&i.,%str( )))  
       %end ;;
    set &FactorsInds. ;
    by variable ;
    %do i = 1 %to &NumScoreVars. ;
      %let var = %scan(&ScoreVars.,&i.,%str( )) ; 
      length &var. %varlen(_allvars_,&var.) ;
      if variable = "&var." then do ;
        &var. = level ;
        output &inLib..Var&i.Lookup ;
      end ;
    %end ;
  run ;

  proc datasets library=&inLib. nolist;
    delete allcombinations temp_ Comb2score ;
  quit ;

  proc fedsql sessref = &casSessionName. ;
    create table &inLib..allCombinations {options compress=true copies=0} as select prods.&product_id_var., 
      users.&Identity_id_Var., users._modeled_identity_
      %do i = 1 %to %words(&Addl_Predictors.) ;
        , users.%scan(&Addl_Predictors.,&i.,%str( )) 
      %end ;
      from &inLib..Product_ids_modeled  as prods, &inLib..Identities2Score as users ;
  quit ;
     
  %if (%length(&IdentityProductExclDs.) > 0) %then %do ;  
    proc fedsql sessref = &casSessionName. ; 
      create table &inLib..temp_ {options compress=true copies=0} as select a.*, case when a.&product_id_var. = b.&product_id_var. then 1 else 0 end as _flag_ 
        from &inLib..allCombinations as a left join &IdentityProductExclDs. as b on 
        a.&product_id_var. = b.&product_id_var. and a.&identity_id_var. = b.&identity_id_var. ;
    
      create table &inLib..Comb2score {options compress=true copies=0} as select &product_id_var., &Identity_id_Var., _modeled_identity_
      %do i = 1 %to %words(&Addl_Predictors.) ;
        , %scan(&Addl_Predictors.,&i.,%str( )) 
      %end ;
      from &inLib..temp_ where _flag_ = 0 ;
    quit ;    
    
    proc datasets library=&Inlib. nolist ;
      delete temp_ allCombinations ;
    quit ;
  %end ;
  %else %do ;
  
    ** rename allcombinations to comb2score **;    
    proc casutil incaslib="&Inlib." outcaslib="&Inlib.";
      altertable casdata="allCombinations" casout="Comb2score" ;  
    quit ;

  %end ;
  
  data &Inlib..Comb2score_astore (compress=YES) ;
    set &inLib..Comb2score (where=(_modeled_identity_=1)) ;
  run ;

  proc astore;
    score data = &inLib..Comb2score_astore  
          out  = &inLib..Modeled_Scored (compress=YES partition=(&identity_id_var.)) 
          rstore = &astoreInds. copyvars=(&identity_id_var. &product_id_var.);
  run;
              
  proc datasets library=&Inlib. nolist ;
    delete Comb2score_astore ;
  quit ;
     
  data &inLib..NoIDentityFactor_scored (compress=YES partition=(&identity_id_var.)) ;
    length 
      %do i = 1 %to &NumScoreVars. ;
        var&i.bias
        %do j = 1 %to &NumFactors ;
          var&i.factor&j. 
        %end ;
      %end ; 8. ;
    if _N_ = 1 then do;
      %do i = 1 %to &NumScoreVars. ;
        %let var = %scan(&ScoreVars.,&i.,%str( )) ; 
          
        declare hash h&i.(dataset:"&inLib..var&i.lookup");                        
        h&i..definekey("&var.");
        h&i..definedata("var&i.bias" %do j = 1 %to &NumFactors ; , "var&i.factor&j." %end;);
        h&i..definedone();
        call missing(var&i.bias %do j = 1 %to &NumFactors ; , var&i.factor&j. %end;);                       
      %end ;
    end;
    set &inLib..Comb2score (where=(_modeled_identity_=0)) ;    
    someMissing = 0 ; 
    %do i = 1 %to &NumScoreVars. ;
      rc&i. = h&i..find();   
      if rc&i. ne 0 then someMissing = 1 ;
    %end ;
     
    %let Score = %str(p_&DependentVar. = &OverAllBias.) ;
     
    %do v = 1 %to &NumScoreVars. ;
      %let Score = &score %str(+ sum(0,var&v.bias)) ;
    %end ;
     
    %do i = 1 %to &numFactors. ;
      %do k = 1 %to %eval(&NumScoreVars. - 1) ;
        %do v = %eval(&k+1) %to &NumScoreVars. ;  
          %let score = &score. %str(+ sum(0,var&k.factor&i.) * sum(0,var&v.factor&i.)) ;
        %end ;
      %end ;
    %end ;
    &score ;
     
   keep &identity_id_var. &product_id_var. p_&DependentVar. ;
  run;

  proc datasets library=&Inlib. nolist ;
    delete Comb2score %do i = 1 %to &NumScoreVars. ; Var&i.Lookup %end ;;
  quit ;

  data &inLib..AllScored (compress=YES partition=(&identity_id_var.) orderby=(DESCENDING p_&DependentVar.));
    set &inLib..NoIDentityFactor_scored 
        &inLib..Modeled_Scored ;
    by &identity_id_var. ;
  run;

  proc datasets library=&Inlib. nolist ;
    delete NoIDentityFactor_scored Modeled_Scored ;
  quit ;
   
  data &RecommendationsOutds. (compress=YES partition=(&identity_id_var.)) ;
    set &inLib..AllScored ;
    by &identity_id_var. DESCENDING p_&DependentVar. ;
    retain rank ;
    if first.&identity_id_var. then Rank = 1;
      else rank = Rank + 1 ;
    if rank <= &NumRecsPerIdentity. then output &RecommendationsOutds. ;
  run;
  
  proc datasets library=&Inlib. nolist ;
    delete allscored ;
  quit ;

  data &RecommendationsOutds. (compress=YES partition=(&identity_id_var.)) ;
    merge &RecommendationsOutds.   (in=inmain)
          &inLib..Identities2Score (in=inadd keep=&identity_id_var. &Addl_Predictors. _modeled_identity_)
    ;
    by &identity_id_var. ;
    if inmain ;
    if not inadd then abort ;
  run ;
  
  %FINISH:  
%mend ;
