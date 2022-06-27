/* Copyright © 2022, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

/* Example patterns

 pos0 = prxmatch('/html/',strip(page_url_txt)) ;  
 pos  = prxmatch('/\.html$/',strip(page_url_txt)) ; 
 pos2 = prxmatch('/\.html(#)(\/*|close)$/',strip(page_url_txt)) ; 

*/

data _NULL_;
  call prxdebug(1);
  if _N_=1 then do;
    retain pattern_ID;
    pattern="/(\.html|\.htm)$/"; /*<--Edit the pattern here.*/
    pattern_ID=prxparse(pattern);
  end;
  input some_data $93.;
  call prxsubstr(pattern_ID, strip(some_data), position, length);
  *if postition = . then abort ;
  if position ^= 0 then do;
    match=substr(some_data, position, length);
    put match:$QUOTE. "found in " some_data:$QUOTE.;
  end;
  call prxdebug(0);
datalines;
https://www.sas.com/en_us/insights/articles/risk-fraud/new-data-on-digital-fraud-trends.html
https:a-on-digital-fraud-trends.html
Smithe, Cindy
103 Pennsylvania Ave. NW, Washington, DC 20216
508 First htmlWashington, DC 20001
650 1st St.htmlashington, DC 20002
3000 K Street NW, Washington, DC 20007
.html.html1560 Wilson Blvd, Arlington, VA 22209html
1-800-123-4567
1(800) 789-1234.html 
run ;