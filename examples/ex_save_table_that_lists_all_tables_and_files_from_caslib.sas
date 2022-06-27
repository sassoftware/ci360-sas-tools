/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

** For a user specified cas library, create two tables **;
** TableMeta - this contains list of all in-memory tables in cas Library **;
** FileMeta  - this contains list of all files in the PATH associated with the cas Library **;
ods output TableInfo=Tablemeta Fileinfo=Filemeta ;
      
proc casutil incaslib="&inlib." SESSREF="my sess";
  list tables;
  list files;
quit ;