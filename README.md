#  SAS Customer Intelligence 360 Analytical SAS Macro Tools

## Overview
This repository is a collection of SAS macros designed to exercise Customer Intelligence 360 (CI360) APIs that download data which is in turn used for analysis and insight.  In addition, there are tools for turning the analytical insights into action by enabling users to upload analytical customer scores, segments and personalization data for execution in CI360:
 
- Leverage proc http and JSON libname to exercise CI360 APIs with SAS code
- Remove duplicates, extract custom attributes, simplify tables and process identities for CI360 data
- Create customer level analytical base tables with features derived from the CI360 Discover and Engage data (e.g. session_details)
- General utilities such as SAS log scanning tool, running sas macro invocations as parallel process, etc 
 
<details><summary>Click here to see a list of the macros organized by thier purpose: </summary>

      CI360 Data download Related (Discover, Engage, Plan, etc)
      - %download_udm_data()
      - %gen_jwt()
      - %throttle_udm_data_download()

      CI360 Data Processing:
      - %combine_base_and_ext_disc_detail()
      - %combine_udm_datasets()
      - %extract_properties_map_doc_cols()
      - %read_udm_data()
      - %remove_udm_data_dups()
      - %update_identities()
      
      Create Customer (identity_id) level summaries for ABT:
      - %make_disc_detail_identity_lvl()      
      - %make_session_identity_lvl()
      - %make_url_vars()      

      CI360 HUB / External Event Related:
      - %batch_load_external_ci360_events()
      - %download_360_hubfiles()
      - %get_360hub_file_metadata()
      - %read_360hub_data()
      - %upload_data2hub()
      
      CI360 Recommender Task Related:
      - %upload_black_white_list()

      Factorization Machines Product Recommender from CI360 Data:
      - %build_prodview_recommender_model()
      - %make_product_recommendations()
      - %make_prodview_abt()

      Integrating CI360 Direct and Optimize:
      - %appendMOSolution()
      - %make_EO_data_from_direct_export()

      Combine CI360 Attribution data with additional external event data:
      - %combine_attribution_data()

      Leverage SAS Marketing Optimization Batch tables to create equivalent proc optmodel code
      - %convert_mo2optmodel()

      General SAS Tools:
      - %call_proc_http()
      - %cas_proc_means()
      - %check_log()
      - %convert_sasdata_to_sashdat()
      - %download_ga_data_from_gbq()
      - %log_run_time()
      - %print_macro_parameters()
      - %run_parallel_jobs()
      - %stopwatch()
      - %tagsort_inmem()
</details>

This topic contains the following sections:
* [Requirements](#requirements)
* [Using the SAS Macros](#using-the-macros)
	* [Exposing the Macros to SAS Session](#exposing-the-macros-to-sas-session)
	* [Examples](#examples)
* [Contributing](#contributing)
* [License](#license)
* [Additional Resources](#additional-resources)


### Requirements
1. Base SAS 9.4M5+ (with Unicode Support) or SAS Viya 3.5+ SAS Studio environment

2. Install a file uncompressing utility that works on gzip files (*.gz) such as 7zip or the gzip program from https://www.gzip.org/.

   After the program is installed, add the location of the gzip program to the PATH environment variable. This is required for SAS program to read .gz files without un-compressing the file.

3. Ideally (but not required) enable SAS to use the XCMD System Option if possible. For more information, see the
     [Help Center for SAS 9.4](https://go.documentation.sas.com/?cdcId=pgmsascdc&cdcVersion=9.4_3.4) and search for the
     XCMD option.
 
4. If necessary, create an access point in SAS Customer Intelligence 360.
    1. From the user interface, navigate to **General Settings** > **External Access** > **Access Points**.
    2. Create a new access point if one does not exist.
    3. Get the all of the following information from the access point:
       ```
        External gateway address: e.g. https://extapigwservice-<server>/marketingGateway  
        Name: ci360_agent  
        Tenant ID: abc123-ci360-tenant-id-xyz  
        Client secret: ABC123ci360clientSecretXYZ
       ```

### Using the Macros ->

#### Exposing the Macros to SAS Session

   You will need to have your own "local" copies of the macros - where "local" means accessible to your SAS/Viya session via 
   SASAUTOS settings set up in the autoexec file.  If you are using EG or Viya, the macros need to be FTP'd to the server 
   running the SAS code.  This is a one time set up that enables SAS to find the macros when you need them.  Here is some 
   example code that would be included in the autoexec file:
   
         options source2 source ;
         options mprint NOQUOTELENMAX msglevel=i ;
         
         %let CC_DIR = /home/sasdemo/macros ;
         
         filename FuncMcro "%superq(CC_DIR)/funcmacr" ;
         filename ProcMcro "%superq(CC_DIR)/procmacr" ;
         
         ** Add funcmacr and procmarc to end of sasautos list **;
         options mautosource insert = (sasautos = ( FuncMcro ProcMcro )) 

#### Examples
Find a list of code samples invoking the macros in the [examples directory](/examples)

## Contributing

We welcome your contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to submit contributions to this project.

## License

This project is licensed under the [Apache 2.0 License](LICENSE).

## Additional Resources
For more information, see [Downloading Data Tables with the REST API](https://go.documentation.sas.com/?softwareId=ONEMKTMID&softwareVersion=production.a&softwareContextId=DownloadDataTables) in the Help Center for SAS Customer Intelligence 360.
