/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0

Use Case1: You have downloaded some time range of CI360 data and you later want to add more time periods 
Use Case2: You downloaded some tables and want to add some additional tables from a later second download

In either case, you want to combine the two separate folders of data into a single combined folder
*/

libname disc     "/original_data_folder" ;
libname disc_new "/new_data_folder" ;

%Combine_UDM_Datasets(Lib2Add      =disc_new,
                      BaseLib      =disc,
                      Tables2Add   =,
                      CompressOutput=Y
                      ) ;