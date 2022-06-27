/*
/ Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
/ SPDX-License-Identifier: Apache-2.0
/============================================================================================
/ Program   : extract_properties_map_doc_cols.sas
/ Author    : Noah Powers
/ Created   : 2022
/ Purpose   : This macro will extract out the individual fields from a CI360 properties_map_doc
/             character column.  Note that the structure of this underlying column can vary from 
/             record to record but will be consistent for a given CI360 object (e.g. task id).
/             Use the where statement macro parameter if needed to restrict to one CI360 object.
/
/ FuncOutput: NA
/ Usage     : %extract_properties_map_doc_cols(inds            =engage.conversion_milestone,                                  
/                                              whereStatement  =%str(task_id="b8622642-8da9-4449-bc63-9bc6faa58e98"),
/                                              NewColNames     =%str(gift amount),
/                                              NewColLengths   =%str($100. 8.),
/                                              NewColInformats =%str($100. comma15.),
/                                              NewColFormats   =%str($100. dollar10.2),
/                                              outds           =engage.test) ;
/ Notes     : 
/============================================================================================
/ Parameters Usage
/ -------------------------------------------------------------------------------------
/ Name                        Description
/ -------------------------------------------------------------------------------------
/ inds                        The name of the SAS inptut dataset that contains document_details data
/ properties_map_doc_col_name The name of the column in the input dataset that contains the
/                             properties_map_doc delimited data.  Default is properties_map_doc.
/ WhereStatement              (optional) This is used to subset the input data
/ NewColNames                 Space delimited list of the SAS variables to create from the 
/                             properties_map_doc column
/ NewColLengths               This contains a space delimited list of the lenghts/types to use for
/                             the new variables created from the properties_map_doc field.
/ NewColInformats             (optional) If this is provided it needs to be provided for ALL 
/                             new columns and is an informat that is applied to the text data.
/                             This is needed for creating numeric columns from formatted text values.
/ NewColFormats               (optional) If this is provided, it needs to be provided for ALL
/                             new columns and is expected to be a list of SAS formats to apply to
/                             the new columns created.
/ outds                       The name of the output SAS dataset to create with the new columns
/ outdsOpts                   (default compress=YES) optional dataset options to apply to the output 
/                             dataset created.
/============================================================================================*/
%macro extract_properties_map_doc_cols(inds                       =,                                  
                                      properties_map_doc_col_name =properties_map_doc,
                                      whereStatement              =,
                                      NewColNames                 =,
                                      NewColLengths               =,
                                      NewColInformats             =,
                                      NewColFormats               =,
                                      outds                       =,
                                      outdsOpts                   =%str(compress=YES)) ;

  %local NumCols cntlB cntlC i name len informat format ;  
  
  %if NOT %length(&whereStatement.) %then %let whereStatement = 1 ;
  
  %let NumCols = %words(&NewColLengths.) ;  
  %let cntlB = '02'x ;
  %let cntlC = '03'x ;
  
  data &outds. (&outdsOpts.) ;
    set &inds. (where=(&whereStatement.));
    
    length 
    %do i = 1 %to &NumCols. ;
      %let name = %scan(&NewColNames.,&i.,%str( )) ;
      %let len = %scan(&NewColLengths.,&i.,%str( )) ;
      &name. &len 
    %end ;
    ;
    
    if length(&properties_map_doc_col_name.) > 0 then do ;
      %do i = 1 %to &NumCols. ;
        %let name = %scan(&NewColNames.,&i.,%str( )) ; 
        %if %length(&NewColInformats.) %then %do ;
          %let informat = %scan(&NewColInformats.,&i.,%str( )) ; 
          &name. = input(scan(scan(&properties_map_doc_col_name.,&i.,&cntlC.),2,&cntlB.),&informat.) ;
        %end ;
        %else %do ;    
          &name. = scan(scan(&properties_map_doc_col_name.,&i.,&cntlC.),2,&cntlB.) ;
        %end ;
      %end ;
    end ;
    
    format
    %if %length(&NewColFormats.) %then %do i = 1 %to &NumCols. ;
      %let name = %scan(&NewColNames.,&i.,%str( )) ;  
      %let format = %scan(&NewColFormats.,&i.,%str( )) ;  
      &name. &format. 
    %end ;
    ;
  run ;
  
%mend ;