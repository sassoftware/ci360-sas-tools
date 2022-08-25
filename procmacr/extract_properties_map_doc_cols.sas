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
/                                              NewColLength   =%str($100.),                                              
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
/                             properties_map_doc column.  This list should match the names found
/                             in the properties_map_col column
/ NewColLength                This contains a the lenght and type to use for all of
/                             the new variables created from the properties_map_doc field.
/ outds                       The name of the output SAS dataset to create with the new columns
/ outdsOpts                   (default compress=YES) optional dataset options to apply to the output 
/                             dataset created.
/============================================================================================*/
%macro extract_properties_map_doc_cols(inds                       =,                                  
                                      properties_map_doc_col_name =properties_map_doc,
                                      whereStatement              =,
                                      NewColNames                 =,
                                      NewColLength                =,                                      
                                      outds                       =,
                                      outdsOpts                   =%str(compress=YES)) ;

  %local NumCols cntlB cntlC i name len informat format ;  
  
  %if NOT %length(&whereStatement.) %then %let whereStatement = 1 ;
  
  %let NumCols = %words(&NewColLength.) ;  
  %let cntlB = '02'x ;
  %let cntlC = '03'x ;
  
  data &outds. (&outdsOpts.) ;
    set &inds. (where=(&whereStatement.));
    array newvars (*) &NewColLength. &newcolnames. ;
  
    _iter = 1 ;
    done = 0 ;
    do while (not done) ;     
      pair = scan(&properties_map_doc_col_name.,_iter,&cntlC.) ;
      if pair <= "" then done = 1 ;
      else _iter = _iter + 1 ;
      _name = scan(pair,1,&cntlB.) ;
      _value = scan(pair,2,&cntlB.) ;
      do i = 1 to dim(newvars) ;
        if upcase(vname(newvars(i))) = upcase(_name) then newvars(i) = strip(_value) ;
      end ;
    end ;
    
    drop _name _value pair _iter done i ;
  run ;
  
%mend ;