/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : Update_Identities.sas
/ Author    : Noah Powers
/ Created   : 2020
/ Purpose   : Update (or create) the value of active_identity_id in each of the SAS
/             datasets found in the DATA_LIB SAS library name provided.  If the 
/             timestamps dataset is found, the sortVars are read and used to preserve
/             the sort order in the output data.  If a value for identifier_type_id_val 
/             is provided, then it will be used to also merge in the user_identifier_val
/             corresponding to the identifier_type_id_val provided.
/ 
/ FuncOutput: NA
/ Usage     : 
/ Notes     : could note those datasets in the library that are not processed 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name               Description
/ -------------------------------------------------------------------------------------
/ data_lib               Required - The name of the SAS library that contains the discover
/                        detail datasets to have identity_id updated.
/ identity_lib           Required - the name of the SAS library that contains the Identity 
/                        data tables
/ identifier_type_id_val (optional) If this paramerer is provided, the user_identifier_val 
/                        column is added to the detail data tables 
/ CompressOutput         (Y/N) The default is Y and this will result in the output SAS datasets
/                        will be compressed. 
/============================================================================================*/
%macro Update_Identities(data_lib              =,
                        identity_lib           =,
                        identifier_type_id_val =,
                        CompressOutput         =Y) ;

  %local dslist Numds i ds droplist OrderByList inViya data_lib_path ident_lib_path sas_data_lib 
         dsopt CASopt ;
  
  %if not %length(&CompressOutput.) %then %let CompressOutput = Y ;
  %let CompressOutput = %upcase(%substr(&CompressOutput.,1,1)) ;
  
  %if "%substr(&sysvlong.,1,1)" = "V" %then %let inViya = 1 ;
  %else %let inViya = 0 ;

  ** list of all datasets in the library that have identity_id **;
  ** note those datasets in the library without identity_id as well for diagnostic purposes **;
  
  proc sql noprint;
    select lowcase(memname) into: dsList separated by " " from sashelp.vcolumn 
    where upcase(libname)="%upcase(&data_lib.)" AND upcase(name)="IDENTITY_ID" ;
  quit ;
  %let NumDs = %words(&dslist.) ;
  
  %let data_lib_path = %sysfunc(pathname(&data_lib.,L)) ;
  %let ident_lib_path = %sysfunc(pathname(&identity_lib.,L)) ;
  %let sas_data_lib = &data_lib. ;
  
  ** If in Viya then load the identity tables needed into CAS **;
  %if (&inviya. = 1) %then %do ;
    cas _tmp_ ;
    
    caslib iddata_ datasource=(srctype=PATH ) path="&ident_lib_path." ;
    libname _idata_ CAS caslib="iddata_" ;
    
    caslib sasdat_ datasource=(srctype=PATH ) path="&data_lib_path." ;
    libname _sasdat_ CAS caslib="sasdat_" ;    
    
    proc casutil incaslib="iddata_" outcaslib="iddata_";
      load casdata="identity_map.sas7bdat" casout="identity_map" replace 
      importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
      %if %length(&identifier_type_id_val.) %then %do ;
        load casdata="identity_attributes.sas7bdat" casout="identity_attributes" replace 
        importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
      %end ;
    quit ;
    
    %let data_lib = _sasdat_ ;
    %let identity_lib = _idata_ ;

  %end ;
  
  %do i = 1 %to &NumDs. ;

    %let ds = %scan(&dslist.,&i.,%str( )) ;
    %let droplist = %match(%varlist(&sas_data_lib..&ds.),active_identity_id user_identifier_val) ;
    %if %length(&droplist.) %then %let droplist = %str(drop=&droplist.) ;
    
    %if (&inviya. = 0) %then %do ;

      ** check if index on identity_id exists - if not create one **;
      data _indx ;
        set sashelp.vindex (where=(upcase(libname)="%upcase(&data_lib.)" AND upcase(memname)="%upcase(&ds.)" AND 
                                   upcase(name)="IDENTITY_ID" AND upcase(idxusage)="SIMPLE")) ;
      run ;
  
      %if %nobs(_indx) = 0 %then %do ;
        proc datasets library=&data_lib. nolist ;
          modify &ds. / memtype=data ;
          index create identity_id ;
        quit ;
      %end ;
    
      %if "&CompressOutput." = "Y" %then %let dsopt = %str(compress=YES) ;
    %end ;
    %else %do ;

      proc casutil incaslib="sasdat_" outcaslib="sasdat_";
        load casdata="&ds..sas7bdat" casout="&ds." replace 
        importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
      quit ;
      
      %let dsopt = ;
    %end ;
    
    data &data_lib..&ds. (&dsopt.) ;
      merge &data_lib..&ds.             (in=inmain &droplist.)
            &identity_lib..identity_map (in=inid keep=source_identity_id target_identity_id rename=(source_identity_id=identity_id))  
      ;
      by identity_id ;
      if inmain ;
      length active_identity_id %varlen(&sas_data_lib..&ds.,identity_id) ;
      active_identity_id = coalesceC(target_identity_id,identity_id) ;
      drop target_identity_id ;
    run ;
    
    %if (&inviya. = 0) %then %do ;

      proc datasets library=&data_lib. nolist ;
        modify &ds. / memtype=data ;
        index create active_identity_id ;
      quit ;
      
    %end ;

    %if %length(&identifier_type_id_val.) %then %do ;
      data &data_lib..&ds. (&dsopt.) ;
        merge &data_lib..&ds.                    (in=inmain)
              &identity_lib..identity_attributes (in=inattr keep=identity_id identifier_type_id user_identifier_val rename=(identity_id=active_identity_id)
                                                 where=(upcase(identifier_type_id)="%upcase(&identifier_type_id_val.)"))
        ;
        by active_identity_id ;
        if inmain ;
        drop identifier_type_id ;
      run ;
    %end ;
    
    %if (&inviya. = 1) %then %do ;
    
      %let CASOpt = ;
      %if "&CompressOutput." = "Y" %then %let CASopt = %str(compress="YES") ;
      
      proc cas;
        table.save /
          caslib="sasdat_"
          name="&ds..sas7bdat"
          table={name="&ds.", caslib="sasdat_"}
          permission="PUBLICWRITE"
          exportOptions={fileType="BASESAS" &CASopt.}
          replace=True;
      quit; 
      
      proc casutil ;
        DROPTABLE CASDATA="&ds." INCASLIB="sasdat_" ;
      quit ;
    %end ;
  %end ;
  
  %if (&inviya. = 1) %then %do ;
     cas _tmp_ terminate ;
  %end ;

%mend ;