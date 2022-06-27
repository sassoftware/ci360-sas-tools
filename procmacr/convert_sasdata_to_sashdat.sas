/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : convert_sasdata_to_sashdat.sas
/ Version   : 1.0
/ Author    : Noah Powers
/ Created   : 2021 May
/ LastModBy : Noah Powers
/ LastModDt : 05.12.2021
/ Purpose   : Detect all sasdata sets (e.g. ending in .sas7bdat) in the sasdataset path.  For
/             each such file, load it into memory/CAS and then save it as sashdat format with 
/             the same name as the original sas dataset but in the sashdat_path folder.
/ FuncOutput: N/A
/ Usage     :
/ Notes     : This code uses proc cas with the permission="PUBLICWRITE" option to ensure that
/             the data is writable by other users.  Currently it is not possible to do 
/             so with Proc casutil - it will write a sashdat that is only writable by the owner
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                    Description
/ -------------------------------------------------------------------------------------
/ sasdata_path The full filepath where the source sasdata sets can be found
/ sashdat_path The full filepath where the sashdat copies will be saved
/ CompressOutput         (Y/N) The default is Y and this will result in the output SAS datasets
/                        will be compressed. 
/============================================================================================*/
%macro convert_sasdata_to_sashdat(sasdata_path  =,
                                  sashdat_path  =,
                                  CompressOutput=Y) ;

  %local NumFiles file_name_list i table CASopt ;
  
  %if not %length(&CompressOutput.) %then %let CompressOutput = Y ;
  %let CompressOutput = %upcase(%substr(&CompressOutput.,1,1)) ;
  %if "&CompressOutput." = "Y" %then %let CASopt = %str(compress=True) ;
  %else %let CASopt = ;
  
  cas _tmp_ ;
  
  caslib sas7 datasource=(srctype=PATH ) path="&sasdata_path." ;
  caslib hdat datasource=(srctype=PATH ) path="&sashdat_path." ;
  
  libname sas7 CAS caslib="sas7" ;
  libname hdat CAS caslib="hdat" ;
  
  ods output Fileinfo=_Filemeta_ ;
        
  proc casutil incaslib="sas7" ;
    list files;
  quit ;
  
  data _Filemeta_ ;
    set _Filemeta_ ;
    File_name = scan(Name,1,".") ;
    FileExt = upcase(scan(Name,2,".")) ;
    if FileExt in ("SAS7BDAT") ;
  run ;
  %let NumFiles  = %nobs(_Filemeta_) ;
  
  proc sql noprint ;
    select file_name into: file_name_list separated by "|" from _filemeta_ ;
  quit ;
  
  %do i = 1 %to &NumFiles. ;
  
    %let table = %scan(&file_name_list.,&i.,%str(|)) ;
  
    proc casutil incaslib="sas7" ;
      load casdata="&table..sas7bdat" casout="&table." outcaslib="sas7" replace 
      importoptions=(filetype="basesas" dataTransferMode="parallel")  ;
    quit ;
  
    proc cas;
      table.save /
        caslib="hdat"
        table= {name="&table.", caslib="sas7"}
        name="&table..sashdat" 
        permission="PUBLICWRITE"
        &CASOpt.
        exportOptions={fileType="AUTO"}
        replace=True ;
    quit;

    proc casutil ;
      droptable casdata="&table." incaslib="sas7" ;
    quit ;

  %end ;
  
  cas _tmp_ terminate ;

%mend ;