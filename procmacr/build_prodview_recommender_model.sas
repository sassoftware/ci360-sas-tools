/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : build_prodview_recommender_model.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2021 July
/ LastModBy : Noah Powers
/ LastModDt : 07.07.2021
/ Purpose   : This macro estimates a factorization machines model (proc factmac) using the
/             user supplied input ABT.  In addition, a second dataset with identities 
/             that have NOT viewed any products is scored across all viewed products to 
/             create recommendations for these non-modeled identities.
/ FuncOutput: N/A
/ Usage     :
/ Notes     : It is bettter for factorization machines model to have transactional style data 
/             with multiple product views if available. So no need to aggregate multiple product 
/             views within or across sessions - but this can be done if desired.
/
/             Do I need to use FREQ option in proc assess when there is weight in factmac?
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name      Description
/ -------------------------------------------------------------------------------------
/ inABT_ds             The full name of the input ABT dataset that will be used with proc factmac
/ Identity_id_Var      default=identity_id The name of the column in the above input datasets
/                      that captures the customer ID or identity ID                   
/ product_id_var       The name of the column in the above input datasets that captures the 
/                      product ID.  Most commonly, this will be product_id or product_sku
/ DependentVar         The name of the column in the INABT_DS that will be predicted in proc
/                      factmac
/ Addl_Predictors      By default, the product_id and the identity_id are used as predictors
/                      but any additional columns provided in this parameter will be used
/                      as predictors in proc factmac.  These must be of type nomimal (not interval) 
/ WeightVar            (optional) If provided, this is the name of the column in the inABT_ds 
/                      that will be used as weight in the proc factmac runs.
/ ValidationPctObs     Default=0.3  The percentage (expressed as a number between 0-1) of observations
/                      to exclude from model fitting data to be used to prevent overfitting the data
/ RandSeed             The seed to use when generating observations for model validation.  A value of
/                      zero (0) uses the current datetime value and will gererate different sets 
/                      of validation obs each run.  While a non-zero value will generate the
/                      same validation obs each run.
/ nFactors             (optional) The number of factors hyperparamter to use with proc factmac.  If 
/                      this parameter (and the other two) are blank then AUTOTUNE is performed and 
/                      the best nFactors value will be used. 
/ maxIter              (optional) The max iterations hyperparamter to use with proc factmac.  If 
/                      this parameter (and the other two) are blank then AUTOTUNE is performed and 
/                      the best nFactors value will be used. 
/ LearnStep            (optional) The learning step hyperparamter to use with proc factmac.  If 
/                      this parameter (and the other two) are blank then AUTOTUNE is performed and 
/                      the best nFactors value will be used. 
/ nonNegative          (Y/N) If this is set to Y then the proc factmac will be run with the 
/                      NONNEGATIVE option on the proc to force the factors to be non-negative
/                      and the biases are all zero
/ FactorsOutds         The estimated coefficients from proc factmac.  This includes the biases (e.g.
/                      intercepts) as well as the factor values 
/ ScoredAbtOutds       The name of the output dataset to create that contains the scored ABT 
/                      records (this excludes observations that were used for validation)
/ astoreOutds          The output name for the astore scoring dataset to be created.
/ fitstatout           The name of the output dataset to contain the fit statistics generated by 
/                      proc assess
/ liftout              The name of the output dataset that contains the lift values from proc assess
/============================================================================================*/
%macro build_prodview_recommender_model(inABT_ds             =,
                                        Identity_id_Var      =identity_id,                        
                                        product_id_var       =,
                                        DependentVar         =,
                                        Addl_Predictors      =,
                                        weightVar            =,
                                        ValidationPctObs     =0.3,
                                        RandSeed             =12345,
                                        nFactors             =,
                                        maxIter              =,
                                        LearnStep            =,
                                        nonNegative          =,
                                        FactorsOutds         =,
                                        ScoredAbtOutds       =,
                                        astoreOutds          =,
                                        fitstatout           =,
                                        liftout              =                                              
                                        ) ; 

  %local inLib inName best_nfactors best_learnstep best_maxiter factMacOpt ;
  
  ** Extract output library name **;
  %let inLib  = %scan(&inABT_ds.,1,%str(.)) ;
  %let inName = %scan(&inABT_ds.,2,%str(.)) ;

  %if not %length(&inName.) %then %do ;
    %let inLib  = WORK ;
    %let inName = &inABT_ds. ;
  %end ;        

  %let nonNegative = %substr(%upcase(&nonNegative.),1,1) ;
  %if ("&nonNegative." = "Y") %then %let factMacOpt = NONNEGATIVE ;
  %else %let factMacOpt = ;

  %if %length(&nFactors.) AND %length(&maxIter.) AND %length(&LearnStep.) %then %do ;
    %let best_nfactors = &nFactors. ;
    %let best_maxIter = &maxIter. ;
    %let best_learnstep = &LearnStep. ;
  %end ;
  %else %do ;

    *ods trace on ; 
  
    ods output fitstat                  =Fitstats 
               tunerResults             =TunerResults 
               BestConfiguration        =BestConfiguration 
               HyperparameterImportance =HyperparameterImportance;
  
    proc factmac data=&inABT_ds. &factMacOpt. ;
      input &Identity_id_Var. &product_id_var. &Addl_Predictors. / level=nominal;
      target &DependentVar. / level=interval;
      %if %length(&weightVar.) %then %do ;
        weight &weightVar. ;
      %end ;
      autotune fraction=&ValidationPctObs.;
    run;  

    options nosyntaxcheck nodmssynchk;
    options obs=MAX replace;
    %let syscc=0; 

    *ods trace off ; 
  
    proc sql noprint ;
      select value into: best_nfactors from BestConfiguration (where=(upcase(name)="NFACTORS")) ;
      select value into: best_learnstep from BestConfiguration (where=(upcase(name)="LEARNSTEP")) ;
      select value into: best_maxiter from BestConfiguration (where=(upcase(name)="MAXITER")) ;
    quit ;

  %end ;

  proc partition data=&inABT_ds. samppct=%sysevalf(100*&ValidationPctObs.) PARTIND seed=&randseed. ;
    output out=&inLib..reco_abt ;
    display 'SRSFreq';
  run;

  proc factmac data=&inLib..reco_abt (where=(_partind_=0)) outmodel=&FactorsOutds. &factMacOpt.
               nfactors=&best_nfactors. learnstep=&best_learnstep. maxiter=&best_maxiter. ;
    input &Identity_id_Var. &product_id_var. &Addl_Predictors. / level=nominal;
    target &DependentVar. / level=interval;
    %if %length(&weightVar.) %then %do ;
      weight &weightVar. ;
    %end ;
    savestate rstore=&astoreOutds. ;
    id &Identity_id_Var. &product_id_var. ;
    output out=&ScoredAbtOutds. copyvars=(_partind_ &Identity_id_Var. &product_id_var. &Addl_Predictors. &DependentVar.);
    *code file=score ;
  run;
  
  proc assess data=&ScoredAbtOutds. fitstatout=&fitstatout. liftout=&liftout.  ;
    var p_&DependentVar.;
    target &DependentVar. / level=interval;
    by _partind_ &product_id_var. ;
  run;

  %FINISH:  
%mend ;