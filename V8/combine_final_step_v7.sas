
%macro main();


%combine(C,rsj);
%sum_step(C,rsj);

%combine(C,rsk);
%sum_step(C,rsk);

%combine(C,rkt);
%sum_step(C,rkt);

%combine(C,rovl);
%sum_step(C,rovl);


%mend main;




%macro sum_step(C,l);



data C.&l;
run;

/*rsj*/
%do i=1 %to 5;

data _null_;
set C.Q_&l&i nobs=nobs;
call symput('e',nobs);
run;

%put &e;

proc sql;
create table sum_Q_&l&i as
select sum(w_aver_q) as total_waq,sum(w_aver_r) as total_war,sum(aver_q) as total_aq, sum(aver_r) as total_ar
from C.Q_&l&i;
quit;

data sum_Q_&l&i;
set sum_Q_&l&i;
war=total_war/&e;
waq=total_waq/&e;
ar=total_ar/&e;
aq=total_aq/&e;
run;

data C.&l;
set C.&l sum_Q_&l&i;
run;

%end;

data C.&l;
set C.&l;
drop total_waq total_war total_aq total_ar;
run;



%mend sum_step;

%macro combine(C,l);

%do i=1 %to 5;

data &&C .Q_&l&i;
set &&C .Q_252_&l&i &&C .Q_502_&l&i &&C .Q_783_&l&i ;
run;

data &&C .Q_&l&i;
set &&C .Q_&l&i;
if w_aver_q='.' then delete;
run;

/**
proc datasets lib=C  nolist;
delete Q_252_&l&i Q_502_&l&i Q_783_&l&i / memtype=data;
quit;
**/

%end;



%mend combine;


%main();
