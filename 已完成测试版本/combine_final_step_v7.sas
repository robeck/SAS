%macro sum_step(ds);

dm log 'clear;' continue;

/*rsj*/
%do i=1 %to 5;

data _null_;
set &&ds .Q_rsj_&i nobs=nobs;
call symput('e',nobs);
run;

%put &e;

proc sql;
create table sum_Q_rsj_&i as
select sum(w_aver_q) as total_waq,sum(w_aver_r) as total_war,sum(aver_q) as total_aq, sum(aver_r) as total_ar
from &&ds .Q_rsj_&i;
quit;

data sum_Q_rsj_&i;
set sum_Q_rsj_&i;
waq=total_waq/&e;
war=total_war/&e;
aq=total_aq/&e;
ar=total_ar/&e;
run;

%end;

data C.rsj;
set sum_Q_rsj_5 sum_Q_rsj_4 sum_Q_rsj_3 sum_Q_rsj_2 sum_Q_rsj_1;
drop total_waq total_war total_aq total_ar;
run;

/*rsk*/
%do i=1 %to 5;

data _null_;
set &&ds .Q_rsk_&i nobs=nobs;
call symput('e',nobs);
run;

%put &e;

proc sql;
create table sum_Q_rsk_&i as
select sum(w_aver_q) as total_waq,sum(w_aver_r) as total_war,sum(aver_q) as total_aq, sum(aver_r) as total_ar
from &&ds .Q_rsk_&i;
quit;

data sum_Q_rsk_&i;
set sum_Q_rsk_&i;
waq=total_waq/&e;
war=total_war/&e;
aq=total_aq/&e;
ar=total_ar/&e;
run;

%end;

data C.rsk;
set sum_Q_rsk_5 sum_Q_rsk_4 sum_Q_rsk_3 sum_Q_rsk_2 sum_Q_rsk_1;
drop total_waq total_war total_aq total_ar;
run;


/*rkt*/
%do i=1 %to 5;

data _null_;
set &&ds .Q_rkt_&i nobs=nobs;
call symput('e',nobs);
run;

%put &e;

proc sql;
create table sum_Q_rkt_&i as
select sum(w_aver_q) as total_waq,sum(w_aver_r) as total_war,sum(aver_q) as total_aq, sum(aver_r) as total_ar
from &&ds .Q_rkt_&i;
quit;

data sum_Q_rkt_&i;
set sum_Q_rkt_&i;
waq=total_waq/&e;
war=total_war/&e;
aq=total_aq/&e;
ar=total_ar/&e;
run;

%end;

data C.rkt;
set sum_Q_rkt_5 sum_Q_rkt_4 sum_Q_rkt_3 sum_Q_rkt_2 sum_Q_rkt_1;
drop total_waq total_war total_aq total_ar;
run;


/*rovl*/
%do i=1 %to 5;

data _null_;
set &&ds .Q_rovl_&i nobs=nobs;
call symput('e',nobs);
run;

%put &e;

proc sql;
create table sum_Q_rovl_&i as
select sum(w_aver_q) as total_waq,sum(w_aver_r) as total_war,sum(aver_q) as total_aq, sum(aver_r) as total_ar
from &&ds .Q_rovl_&i;
quit;

data sum_Q_rovl_&i;
set sum_Q_rovl_&i;
waq=total_waq/&e;
war=total_war/&e;
aq=total_aq/&e;
ar=total_ar/&e;
run;

%end;

data C.rovl;
set sum_Q_rovl_5 sum_Q_rovl_4 sum_Q_rovl_3 sum_Q_rovl_2 sum_Q_rovl_1;
drop total_waq total_war total_aq total_ar;
run;


%mend sum_step;

%macro combine(C);

%do i=1 %to 5;

data &&C .Q_rsj_&i;
set &&C .Q_rsj_252_&i &&C .Q_rsj_502_&i &&C .Q_rsj_783_&i ;
run;

data &&C .Q_rsj_&i;
set &&C .Q_rsj_&i;
if w_aver_q='.' then delete;
run;

proc datasets lib=C  nolist;
delete Q_rsj_252_&i Q_rsj_502_&i Q_rsj_783_&i / memtype=data;
quit;


%end;



%do i=1 %to 5;

data &&C .Q_rsk_&i;
set &&C .Q_rsk_252_&i &&C .Q_rsk_502_&i &&C .Q_rsk_783_&i ;
run;

data &&C .Q_rsk_&i;
set &&C .Q_rsk_&i;
if w_aver_q='.' then delete;
run;

proc datasets lib=C  nolist;
delete Q_rsk_252_&i Q_rsk_502_&i Q_rsk_783_&i / memtype=data;
quit;


%end;

%do i=1 %to 5;

data &&C .Q_rkt_&i;
set &&C .Q_rkt_252_&i &&C .Q_rkt_502_&i &&C .Q_rkt_783_&i ;
run;

data &&C .Q_rkt_&i;
set &&C .Q_rkt_&i;
if w_aver_q='.' then delete;
run;

proc datasets lib=C  nolist;
delete Q_rkt_252_&i Q_rkt_502_&i Q_rkt_783_&i / memtype=data;
quit;


%end;


%do i=1 %to 5;

data &&C .Q_rovl_&i;
set &&C .Q_rovl_252_&i &&C .Q_rovl_502_&i &&C .Q_rovl_783_&i ;
run;

data &&C .Q_rovl_&i;
set &&C .Q_rovl_&i;
if w_aver_q='.' then delete;
run;

proc datasets lib=C  nolist;
delete Q_rovl_252_&i Q_rovl_502_&i Q_rovl_783_&i / memtype=data;
run;
quit;


%end;

%sum_step(C);

%mend combine;

%combine(C);
