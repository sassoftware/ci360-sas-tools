/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

%let DSC_AUTH_TOKEN = ;
%Gen_JWT(tenant_id         = %str(<tenant ID>),
         secret_key        = %str(<secret key>),
         method            = datastep,
         out_macrovar_name = DSC_AUTH_TOKEN) ;
         
** When you already have a data descriptor uploaded (and have ID) **;
%Upload_Data2HUB(JWT                    =%superq(DSC_AUTH_TOKEN),
                 DataDescriptorID       =%str(<Descriptor ID>),
                 DataDescriptorJsonFile =,
                 File2Import            =%str(D:\Temp\noahtest.csv),             
                 headerRowIncluded      =%str(true),
                 updateMode             =upsert,
                 contentName            =%str(Noah first upload)) ;

** When you have the JSON discriptor to upload and create descriptor ID **;
%Upload_Data2HUB(JWT                    =%superq(DSC_AUTH_TOKEN),
                 DataDescriptorID       =,
                 DataDescriptorJsonFile =%str(D:\Temp\Data Descriptor.txt),
                 File2Import            =%str(D:\Temp\noahtest.csv),             
                 headerRowIncluded      =%str(true),
                 updateMode             =upsert,
                 contentName            =%str(Noah second upload)) ;