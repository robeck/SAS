%macro sum_step(ds);


%do i=1 %to 5;

data _null_;
set &&ds .W_&i nobs=nobs;
call symput('e',nobs);
run;

%put &e;

proc sql;
create table sum_w_&i as
select sum(w_aver_q) as total_waq,sum(w_aver_r) as total_war,sum(aver_q) as total_aq, sum(aver_r) as total_ar
from &&ds .W_&i;
quit;

data sum_w_&i;
set sum_w_&i;
waq=total_waq/&e;
war=total_war/&e;
aq=total_aq/&e;
ar=total_ar/&e;
run;

%end;



%mend sum_step;

%sum_step(Rsj);
