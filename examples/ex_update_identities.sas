/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

libname _dat "/engage" ;
libname _ids "/identity" ;

%Update_Identities(data_lib               =_dat,
                   identity_lib           =_ids,
                   identifier_type_id_val =) ;
                   
                           