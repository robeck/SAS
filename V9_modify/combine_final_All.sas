
%macro main();

/*double*/
/*rsj control by rsk*/
%double(_rsj);
/*rsk control by rsj*/
%double(_rsk);

/*single*/
%combine(D,rsj);
%sum_step(D,rsj);

%combine(D,rsk);
%sum_step(D,rsk);

%combine(D,rkt);
%sum_step(D,rkt);

%combine(D,rovl);
%sum_step(D,rovl);


%combine(D,resid_rsj_rsk);
%sum_step(D,resid_rsj_rsk);


%combine(D,resid_rsk_rsj);
%sum_step(D,resid_rsk_rsj);

/*cross_section*/
%cross_reg();

%mend main;




%macro sum_step(C,l);



/*rsj*/
%do i=1 %to 5;

data _null_;
set &&C .Q_&l&i nobs=nobs;
call symput('e',nobs);
run;

%put &e;

proc sql;
create table sum_Q_&l&i as
select sum(w_aver_q) as total_waq,sum(w_aver_r) as total_war,sum(aver_q) as total_aq, sum(aver_r) as total_ar
from &&C .Q_&l&i;
quit;

data sum_Q_&l&i;
set sum_Q_&l&i;
war=total_war/&e;
waq=total_waq/&e;
ar=total_ar/&e;
aq=total_aq/&e;
run;


%end;

%let t=1;
%let y=2;
%let g=3;
%let h=4;
%let b=5;

/*排序中是升序排列，需要降序组合即从sum_q_1到sum_q_5*/
/*排序中是降序排列，需要降序组合即从sum_q_5到sum_q_1*/
data &&C .&l;
set sum_Q_&l&t sum_Q_&l&y sum_Q_&l&g sum_Q_&l&h sum_Q_&l&b;
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

%macro cross_reg();

data reg;
set E.reg_252 E.reg_502 E.reg_783;
run;

data E.reg;
set reg;
if rsj='.' then delete;
run;

data _null_;
set E.reg nobs=nobs;
call symput('e',nobs);
run;


proc sql;
create table E.reg_all as 
select sum(rsj) as to_rsj,sum(rsk) as to_rsk,sum(rkt) as to_rkt,sum(rovl) as to_rovl
from E.reg;
quit;

data E.reg_all(keep=ave_rsj ave_rsk ave_rkt ave_rovl);
set E.reg_all;
ave_rsj=to_rsj/&e;
ave_rsk=to_rsk/&e;
ave_rkt=to_rkt/&e;
ave_rovl=to_rovl/&e;
run;




%mend cross_reg;

%macro double(v);

%let m=_252;
%let n=_502;
%let p=_783;

%do i=1 %to 5;
	%do j=1 %to 5;
 		data C.w_&i&j&v;
		set C.w_&i&j&m&v C.w_&i&j&n&v C.w_&i&j&p&v;
		run;

		data C.w_&i&j&v;
		set C.w_&i&j&v;
		if w_aver_q='.' then delete;
		run;

		data _null_;
		set C.w_&i&j&v nobs=nobs;
		call symput('w',nobs);
		run;

		proc sql;
		create table C.wm_&i&j&v as
		select sum(w_aver_q) as total_waq,sum(w_aver_r) as total_war,sum(aver_q) as total_aq, sum(aver_r) as total_ar
		from C.w_&i&j&v;
		quit;


		data C.wm_&i&j&v;
		set C.wm_&i&j&v;
		war=total_war/&w;
		waq=total_waq/&w;
		ar=total_ar/&w;
		aq=total_aq/&w;
		run;

	%end;
%end;




%mend double;


%main();
