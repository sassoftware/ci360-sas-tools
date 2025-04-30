/*
https://go.documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/lefunctionsref/n1mlc3f9w9zh9fn13qswiq6hrta0.htm
*/


%let repoLoc = /create-export/create/homes/Noah.Powers@sas.com/sas_code_ex_github ;

%let github_key = ;

filename key "/create-export/create/homes/Noah.Powers@sas.com/github_key.txt" ;
data _null_ ;
  infile key dsd MISSOVER LRECL=60000;
  length key $2000 ;
  input key $ ;
  *put key= ;
  call symput("github_key",trim(left(key))) ;
run ;


/* This code looks for changes and writes them to a table (as well as to the log) */
data RepoChanges ;
  length path $200. status staged $20 ;
  numChanges = gitfn_status("&repoLoc.");
  put numChanges=;
  do i = 1 to numChanges ;
    rc_status = gitfn_status_get(i,"&repoLoc.","STATUS",status);
    rc_path = gitfn_status_get(i,"&repoLoc.","PATH",path);
    rc_staged = gitfn_status_get(i,"&repoLoc.","STAGED",staged);
    output ;
  end ;
  drop i ;
run;

/* This code looks for changes, writes to a table, and stages any modified files */
data RepoChanges ;
  length path $200. status staged $20 ;
  numChanges = gitfn_status("&repoLoc.");
  put numChanges=;
  do i = 1 to numChanges ;
    rc_status = gitfn_status_get(i,"&repoLoc.","STATUS",status);
    rc_path = gitfn_status_get(i,"&repoLoc.","PATH",path);
    rc_staged = gitfn_status_get(i,"&repoLoc.","STAGED",staged);
    if substr(staged,1,1) = "F" then do ;
      rc = gitfn_idx_add("&repoLoc.",strip(path),strip(status)) ;
    end ;
    output ;
  end ;
  rc = gitfn_status("&repoLoc.");
  drop i rc ;
run;

data _null_;
 rc = gitfn_commit(                   
    "&repoLoc.",          
    "HEAD",          
    "sasMazter",                      
    "noah.powers@sas.com",                     
    "your-commit-message");           
   put rc=;
run;

data _null_;
 rc= git_push(                    
  "&repoLoc.",        
  'sasmazter',            
  "&github_key.");           
run;


%let repoLoc = /create-export/create/homes/Noah.Powers@sas.com/new_location ;
data _null_;
    rc = gitfn_clone (               
     "https://github.com/sassoftware/ci360-sas-tools.git",                    
     "&repoLoc.",
     'sasmazter',            
     "&github_key.");            
    put rc=;                         
run;