data A.info_3;
set A.info_3;
rename complete_cusip=cusip;
run;

proc sql noprint;
create table A.name_info_new as
select *
from A.name_info as a left join A.info_3 as b on a.cusip_id=b.cusip;
quit;

data A.name_info_new;
set A.name_info_new;
coup=coupon*principal_amt*0.01;
run;

data A.name_info_new;
set A.name_info_new;
rename coup=coupon;
run;
