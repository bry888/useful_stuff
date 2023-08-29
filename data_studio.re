CASE WHEN REGEXP_MATCH(format, '(?i).*Showcase.MOBILE.*') THEN 'Showcase MOBILE' 
WHEN REGEXP_MATCH(format, '(?i).*Showcase.MAINPAGE.(RWD.)?1.*') THEN 'Showcase MAINPAGE 1' 
WHEN REGEXP_MATCH(format, '(?i).*Showcase.MAINPAGE.(RWD.)?2.*') THEN 'Showcase MAINPAGE 2' 
WHEN REGEXP_MATCH(format, '(?i).*Showcase.MOTO.RIGHT.*') THEN 'Showcase MOTO RIGHT'
WHEN REGEXP_MATCH(format, '(?i).*Parallax.*') THEN 'Parallax'
ELSE 'other' END

use (?i)email to match any all case variations of “email”. > case insensitive!
United (States|Kingdom) matches both “United States” and “United Kingdom”

DATA STUDIO escape char: REGEXP_EXTRACT(Strona, 'blog\\/([a-zA-Z0-9-]*)\\.html')


SUM(CASE WHEN REGEXP_MATCH(creative, '.*brandedshop.*') AND NOT REGEXP_MATCH(creative, '.*logo.*') THEN 0 ELSE impressions END)
