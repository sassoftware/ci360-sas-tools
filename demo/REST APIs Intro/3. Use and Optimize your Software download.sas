/* 

  Including legal disclaimer to comply with SAS company policy:
  SAS INSTITUTE INC. IS PROVIDING YOU WITH THE COMPUTER SOFTWARE CODE INCLUDED WITH THIS AGREEMENT ("CODE") ON AN "AS IS" BASIS, AND AUTHORIZES YOU TO USE THE CODE SUBJECT TO THE TERMS HEREOF. BY USING THE CODE, YOU AGREE TO THESE TERMS. YOUR USE OF THE CODE IS AT YOUR OWN RISK. SAS INSTITUTE INC. MAKES NO REPRESENTATION OR WARRANTY, EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT AND TITLE, WITH RESPECT TO THE CODE.
   
  The Code is intended to be used solely as part of a product ("Software") you currently have licensed from SAS Institute Inc. or one of its subsidiaries or authorized agents ("SAS"). The Code is designed to either correct an error in the Software or to add functionality to the Software, but has not necessarily been tested. Accordingly, SAS makes no representation or warranty that the Code will operate error-free.  SAS is under no obligation to maintain or support the Code.
   
  Neither SAS nor its licensors shall be liable to you or any third party for any general, special, direct, indirect, consequential, incidental or other damages whatsoever arising out of or related to your use or inability to use the Code, even if SAS has been advised of the possibility of such damages.
   
  Except as otherwise provided above, the Code is governed by the same agreement that governs the Software. If you do not have an existing agreement with SAS governing the Software, you may not use the Code.

   This example is just another PROC HTTP use case but it does NOT leverage any REST APIs (just scraping text from the underlying HTML page)
*/
filename htm "/innovationlab-export/innovationlab/homes/Noah.Powers@sas.com/tmp/use and optimize your software.html" ;

proc http url="https://www.sas.com/en_us/learn/software-success.html" out=htm ;
run ;

data train_details ;
  infile htm lrecl=32767 length=recLen end=lastrec stopover ; 
  input line_in $varying32767. recLen ;
  lineNum = _N_ ;
  retain found_links 0 url alt_title_txt title_txt description ;
  length url $200. alt_title_txt title_txt $100. description $500. ;

  start_url_pos = index(line_in,'<a href="') + 9 ;
  end_url_pos = index(line_in,'" target="_self">') - 1 ;
  if start_url_pos > 9 and end_url_pos > -1 then do ;
    url = substr(line_in,start_url_pos,end_url_pos - start_url_pos + 1) ;
    *output ;
  end ;

  search_string1 = '</span><span class="title">' ;
  end_alt_title_pos = index(line_in,search_string1) -1 ;

  if end_alt_title_pos > -1 then do ;
    alt_title_txt = substr(line_in,1,end_alt_title_pos) ;
    *output ;
  end ;

  search_string = '</span><span class="is-visible abstract">' ;
  end_title_pos = index(line_in,strip(search_string)) -1 ;

  if end_title_pos > -1 then do ;
    title_txt = substr(line_in, 1, end_title_pos) ;
    description = substr(line_in,end_title_pos + length(search_string)+1) ;
    description = substr(description,1,length(description) -7) ;
    output ;
  end ;
   
  keep url alt_title_txt title_txt description ;
run ;
