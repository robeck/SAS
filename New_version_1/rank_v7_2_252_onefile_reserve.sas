/*

1.�����ո����������qunity����ͬһ�ļ����У����ڱ��ʹ��
2.����residual�����������պ�ʹ��






*/


%macro rank(t);


%let M_week=252;



/*���������ݼ��ϲ���ʱ�������ڴ����������һ�κϲ����ݼ�����������֤����˳��*/
%do j=2 %to &M_week;
data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk resid_rsk_rsj resid_rsj_rsk id p_n);
set %do i=1 %to 5000;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;


data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk id p_n);
set rank_&j %do i=5001 %to 10000;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;


data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk id p_n);
set rank_&j %do i=10001 %to 15000;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;

data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk id p_n);
set rank_&j %do i=15001 %to 20000;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;

data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk id p_n);
set rank_&j %do i=20001 %to 25000;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;

data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk id p_n);
set rank_&j %do i=25001 %to 29608;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;



/*rank by rsk*/

/*V4����*/


%end;

dm log 'clear;' continue;

/*�����������з��� kΪ���� hΪѡȡ������*/
/*rank by rsj*/
%split_rsj(5,&M_week);
dm log 'clear;' continue;
%t_rsj(5,&M_week);
dm log 'clear;' continue;

/*rank by rsk*/
%split_rsk(5,&M_week);
dm log 'clear;' continue;
%t_rsk(5,&M_week);
dm log 'clear;' continue;

/*rank by rkt*/
%split_rkt(5,&M_week);
dm log 'clear;' continue;
%t_rkt(5,&M_week);
dm log 'clear;' continue;

/*rank by rovl*/
%split_rovl(5,&M_week);
dm log 'clear;' continue;
%t_rovl(5,&M_week);
dm log 'clear;' continue;


%mend rank;
/*��������ÿ�����ݼ� ÿ��������t��ծȯ����5�ܵ����ݼ�h�ܣ��۲�ÿ���ж��ٸ�ծȯnobsȻ�����k�������ݼ�*/
%macro split_rsj(k, h);


%do l=2 %to &h;

proc sort data=rank_&l;
by descending rsj;
run;

data rank_&l;
set rank_&l;
if r_rf='.' then delete;
run;

data rank_test_&l;
set rank_&l end=eof nobs=count;
if eof then call symput('nobs', left(count));
run;


/*����ȷ�������ծȯ����ȡ�����������ݼ���21��ծȯ�������飬��ôÿ��ȡ6��ծȯ�����һ��Ϊʣ�µ�*/
%let n=%sysfunc(int(%sysevalf(&nobs/&k,ceil)));

%put &nobs &n &l;
dm log 'clear;' continue;
/*ÿ�����ݼ���n=nobs/k��ծȯ
  ��һ�������ݽ��з������*/
%sep(&k,&n,&l);
dm log 'clear;' continue;

%end;


%mend split_rsj;


%macro split_rsk(k, h);


%do l=2 %to &h;

proc sort data=rank_&l;
by descending rsk;
run;

data rank_&l;
set rank_&l;
if r_rf='.' then delete;
run;

data rank_test_&l;
set rank_&l end=eof nobs=count;
if eof then call symput('nobs', left(count));
run;


/*����ȷ�������ծȯ����ȡ�����������ݼ���21��ծȯ�������飬��ôÿ��ȡ6��ծȯ�����һ��Ϊʣ�µ�*/
%let n=%sysfunc(int(%sysevalf(&nobs/&k,ceil)));

%put &nobs &n &l;
dm log 'clear;' continue;
/*ÿ�����ݼ���n=nobs/k��ծȯ
  ��һ�������ݽ��з������*/
%sep(&k,&n,&l);
dm log 'clear;' continue;

%end;


%mend split_rsk;


%macro split_rkt(k, h);


%do l=2 %to &h;

proc sort data=rank_&l;
by descending rkt;
run;

data rank_&l;
set rank_&l;
if r_rf='.' then delete;
run;

data rank_test_&l;
set rank_&l end=eof nobs=count;
if eof then call symput('nobs', left(count));
run;


/*����ȷ�������ծȯ����ȡ�����������ݼ���21��ծȯ�������飬��ôÿ��ȡ6��ծȯ�����һ��Ϊʣ�µ�*/
%let n=%sysfunc(int(%sysevalf(&nobs/&k,ceil)));

%put &nobs &n &l;
dm log 'clear;' continue;
/*ÿ�����ݼ���n=nobs/k��ծȯ
  ��һ�������ݽ��з������*/
%sep(&k,&n,&l);
dm log 'clear;' continue;

%end;


%mend split_rkt;

%macro split_rovl(k, h);


%do l=2 %to &h;

proc sort data=rank_&l;
by descending rovl;
run;

data rank_&l;
set rank_&l;
if r_rf='.' then delete;
run;

data rank_test_&l;
set rank_&l end=eof nobs=count;
if eof then call symput('nobs', left(count));
run;


/*����ȷ�������ծȯ����ȡ�����������ݼ���21��ծȯ�������飬��ôÿ��ȡ6��ծȯ�����һ��Ϊʣ�µ�*/
%let n=%sysfunc(int(%sysevalf(&nobs/&k,ceil)));

%put &nobs &n &l;
dm log 'clear;' continue;
/*ÿ�����ݼ���n=nobs/k��ծȯ
  ��һ�������ݽ��з������*/
%sep(&k,&n,&l);
dm log 'clear;' continue;

%end;


%mend split_rovl;

/*kΪ������nΪÿ����������ծȯ��lΪ��ǰ��
���ÿ������õ����ݼ��ᱻ��ֳ�������k��*/
%macro sep(k,n,l);

%let data1=data_&l
;
%do m=1 %to &k;
data &data1&m rank_test_&l;
set rank_test_&l;
if _N_<=&n then output &data1&m;
else output rank_test_&l;
%delet(rank_test_&l);
run;

dm log 'clear;' continue;

%end;

%mend sep;



/*ɾ���۲�Ϊ0�����ݼ�*/
%macro delet(ds);

data _null_;
if 0 then set &ds  nobs=count;
call symput('obs', count);
run;


%put &obs;

%if &obs=0 %then %do;
		proc datasets lib=work nolist;
        delete &ds;
        quit;
%end;

dm log 'clear;' continue;

%mend delet;

/*split��ɺ����ǵõ�ÿ�ܣ��ֳ�5�ݵ����ݼ�*/
%macro t_rsj(r,s);

/*���ﴴ�������ݼ�Ϊ����5��ʱ*/
data C.Q_rsj_252_1 C.Q_rsj_252_2 C.Q_rsj_252_3 C.Q_rsj_252_4 C.Q_rsj_252_5;
run;


%do i=2 %to &s;
%let data1=data_&i;
%let name1=name_&i;


%let f=%eval(&i+1);
%let weight_q_r=weight_q_r_&f;
%let w_q_r=w_q_r_&f;
%let w=w_&f;

%do j=1 %to &r;
/*�������Ǹ���ǰһ�ܵ������������һ�ܵģ���������ȡǰһ�ܵ�id��Ϣ*/
data &name1&j;
set &data1&j (keep=cusip_id);
run;

/*ƥ��ǰһ�ܵ�id��Ϣ�뵱ǰ�ܵ�����*/
proc sql;
create table &weight_q_r&j as
select *,sum(p_n) as total from &name1&j as a left join rank_&f as b on a.cusip_id=b.cusip_id;

quit;


/*����Ȩ�أ����Ǽ��㰣��������ھ�ֵ �� return�����ھ�ֵ*/

data &weight_q_r&j;
set &weight_q_r&j;
if Q=' ' then delete;
weight=(p_n)/(total);
q_w=Q*weight;
r_rf_w=r_rf*weight;
run;

%delet(&weight_q_r&j);

proc sql;
create table &w_q_r&j as
select *,sum(q_w) as QW,sum(r_rf_w) as RRFW,sum(Q) as QnW,sum(r_rf) as r_rfnW
from &weight_q_r&j;
quit;


data &w_q_r&j;
set &w_q_r&j nobs=nobs;
w_aver_q=QW/nobs;
w_aver_r=RRFW/nobs;
aver_q=QnW/nobs;
aver_r=r_rfnW/nobs;
run;

%delet(&w_q_r&j);

/*������ȡÿ�ܵľ�ֵ��ϳ�һ�����ݼ������Ҫ����ƽ������Ŀǰ��û����*/
proc sql;
create table &w&j as
select distinct w_aver_q, w_aver_r, aver_q, aver_r
from &w_q_r&j;
quit;

%if &j=1 %then %do;
data C.Q_rsj_252_1;
set C.Q_rsj_252_1 &w&j;
run;
%end;

%if &j=2 %then %do;
data C.Q_rsj_252_2;
set C.Q_rsj_252_2 &w&j;
run;
%end;

%if &j=3 %then %do;
data C.Q_rsj_252_3;
set C.Q_rsj_252_3 &w&j;
run;
%end;

%if &j=4 %then %do;
data C.Q_rsj_252_4;
set C.Q_rsj_252_4 &w&j;
run;
%end;

%if &j=5 %then %do;
data C.Q_rsj_252_5;
set C.Q_rsj_252_5 &w&j;
run;
%end;


dm log 'clear;' continue;


proc datasets lib=work  nolist;
		delete &name1&j &data1&j &weight_q_r&j &w_q_r&j &w&j/ memtype=data;
		quit;



%end;
proc datasets lib=work  nolist;
		delete rank_test_&i / memtype=data;
		quit;

dm log 'clear;' continue;


%end;

%mend t_rsj;


%macro t_rsk(r,s);

/*���ﴴ�������ݼ�Ϊ����5��ʱ*/
data C.Q_rsk_252_1 C.Q_rsk_252_2 C.Q_rsk_252_3 C.Q_rsk_252_4 C.Q_rsk_252_5;
run;


%do i=2 %to &s;
%let data1=data_&i;
%let name1=name_&i;


%let f=%eval(&i+1);
%let weight_q_r=weight_q_r_&f;
%let w_q_r=w_q_r_&f;
%let w=w_&f;

%do j=1 %to &r;
/*�������Ǹ���ǰһ�ܵ������������һ�ܵģ���������ȡǰһ�ܵ�id��Ϣ*/
data &name1&j;
set &data1&j (keep=cusip_id);
run;

/*ƥ��ǰһ�ܵ�id��Ϣ�뵱ǰ�ܵ�����*/
proc sql;
create table &weight_q_r&j as
select *,sum(p_n) as total from &name1&j as a left join rank_&f as b on a.cusip_id=b.cusip_id;

quit;


/*����Ȩ�أ����Ǽ��㰣��������ھ�ֵ �� return�����ھ�ֵ*/

data &weight_q_r&j;
set &weight_q_r&j;
if Q=' ' then delete;
weight=(p_n)/(total);
q_w=Q*weight;
r_rf_w=r_rf*weight;
run;

%delet(&weight_q_r&j);

proc sql;
create table &w_q_r&j as
select *,sum(q_w) as QW,sum(r_rf_w) as RRFW,sum(Q) as QnW,sum(r_rf) as r_rfnW
from &weight_q_r&j;
quit;


data &w_q_r&j;
set &w_q_r&j nobs=nobs;
w_aver_q=QW/nobs;
w_aver_r=RRFW/nobs;
aver_q=QnW/nobs;
aver_r=r_rfnW/nobs;
run;

%delet(&w_q_r&j);

/*������ȡÿ�ܵľ�ֵ��ϳ�һ�����ݼ������Ҫ����ƽ������Ŀǰ��û����*/
proc sql;
create table &w&j as
select distinct w_aver_q, w_aver_r, aver_q, aver_r
from &w_q_r&j;
quit;

%if &j=1 %then %do;
data C.Q_rsk_252_1;
set C.Q_rsk_252_1 &w&j;
run;
%end;

%if &j=2 %then %do;
data C.Q_rsk_252_2;
set C.Q_rsk_252_2 &w&j;
run;
%end;

%if &j=3 %then %do;
data C.Q_rsk_252_3;
set C.Q_rsk_252_3 &w&j;
run;
%end;

%if &j=4 %then %do;
data C.Q_rsk_252_4;
set C.Q_rsk_252_4 &w&j;
run;
%end;

%if &j=5 %then %do;
data C.Q_rsk_252_5;
set C.Q_rsk_252_5 &w&j;
run;
%end;


dm log 'clear;' continue;


proc datasets lib=work  nolist;
		delete &name1&j &data1&j &weight_q_r&j &w_q_r&j &w&j/ memtype=data;
		quit;



%end;
proc datasets lib=work  nolist;
		delete rank_test_&i / memtype=data;
		quit;

dm log 'clear;' continue;


%end;

%mend t_rsk;

%macro t_rkt(r,s);

/*���ﴴ�������ݼ�Ϊ����5��ʱ*/
data C.Q_rkt_252_1 C.Q_rkt_252_2 C.Q_rkt_252_3 C.Q_rkt_252_4 C.Q_rkt_252_5;
run;


%do i=2 %to &s;
%let data1=data_&i;
%let name1=name_&i;


%let f=%eval(&i+1);
%let weight_q_r=weight_q_r_&f;
%let w_q_r=w_q_r_&f;
%let w=w_&f;

%do j=1 %to &r;
/*�������Ǹ���ǰһ�ܵ������������һ�ܵģ���������ȡǰһ�ܵ�id��Ϣ*/
data &name1&j;
set &data1&j (keep=cusip_id);
run;

/*ƥ��ǰһ�ܵ�id��Ϣ�뵱ǰ�ܵ�����*/
proc sql;
create table &weight_q_r&j as
select *,sum(p_n) as total from &name1&j as a left join rank_&f as b on a.cusip_id=b.cusip_id;

quit;


/*����Ȩ�أ����Ǽ��㰣��������ھ�ֵ �� return�����ھ�ֵ*/

data &weight_q_r&j;
set &weight_q_r&j;
if Q=' ' then delete;
weight=(p_n)/(total);
q_w=Q*weight;
r_rf_w=r_rf*weight;
run;

%delet(&weight_q_r&j);

proc sql;
create table &w_q_r&j as
select *,sum(q_w) as QW,sum(r_rf_w) as RRFW,sum(Q) as QnW,sum(r_rf) as r_rfnW
from &weight_q_r&j;
quit;


data &w_q_r&j;
set &w_q_r&j nobs=nobs;
w_aver_q=QW/nobs;
w_aver_r=RRFW/nobs;
aver_q=QnW/nobs;
aver_r=r_rfnW/nobs;
run;

%delet(&w_q_r&j);

/*������ȡÿ�ܵľ�ֵ��ϳ�һ�����ݼ������Ҫ����ƽ������Ŀǰ��û����*/
proc sql;
create table &w&j as
select distinct w_aver_q, w_aver_r, aver_q, aver_r
from &w_q_r&j;
quit;

%if &j=1 %then %do;
data C.Q_rkt_252_1;
set C.Q_rkt_252_1 &w&j;
run;
%end;

%if &j=2 %then %do;
data C.Q_rkt_252_2;
set C.Q_rkt_252_2 &w&j;
run;
%end;

%if &j=3 %then %do;
data C.Q_rkt_252_3;
set C.Q_rkt_252_3 &w&j;
run;
%end;

%if &j=4 %then %do;
data C.Q_rkt_252_4;
set C.Q_rkt_252_4 &w&j;
run;
%end;

%if &j=5 %then %do;
data C.Q_rkt_252_5;
set C.Q_rkt_252_5 &w&j;
run;
%end;


dm log 'clear;' continue;


proc datasets lib=work  nolist;
		delete &name1&j &data1&j &weight_q_r&j &w_q_r&j &w&j/ memtype=data;
		quit;



%end;
proc datasets lib=work  nolist;
		delete rank_test_&i / memtype=data;
		quit;

dm log 'clear;' continue;


%end;

%mend t_rkt;

%macro t_rovl(r,s);

/*���ﴴ�������ݼ�Ϊ����5��ʱ*/
data C.Q_rovl_252_1 C.Q_rovl_252_2 C.Q_rovl_252_3 C.Q_rovl_252_4 C.Q_rovl_252_5;
run;


%do i=2 %to &s;
%let data1=data_&i;
%let name1=name_&i;


%let f=%eval(&i+1);
%let weight_q_r=weight_q_r_&f;
%let w_q_r=w_q_r_&f;
%let w=w_&f;

%do j=1 %to &r;
/*�������Ǹ���ǰһ�ܵ������������һ�ܵģ���������ȡǰһ�ܵ�id��Ϣ*/
data &name1&j;
set &data1&j (keep=cusip_id);
run;

/*ƥ��ǰһ�ܵ�id��Ϣ�뵱ǰ�ܵ�����*/
proc sql;
create table &weight_q_r&j as
select *,sum(p_n) as total from &name1&j as a left join rank_&f as b on a.cusip_id=b.cusip_id;

quit;


/*����Ȩ�أ����Ǽ��㰣��������ھ�ֵ �� return�����ھ�ֵ*/

data &weight_q_r&j;
set &weight_q_r&j;
if Q=' ' then delete;
weight=(p_n)/(total);
q_w=Q*weight;
r_rf_w=r_rf*weight;
run;

%delet(&weight_q_r&j);

proc sql;
create table &w_q_r&j as
select *,sum(q_w) as QW,sum(r_rf_w) as RRFW,sum(Q) as QnW,sum(r_rf) as r_rfnW
from &weight_q_r&j;
quit;


data &w_q_r&j;
set &w_q_r&j nobs=nobs;
w_aver_q=QW/nobs;
w_aver_r=RRFW/nobs;
aver_q=QnW/nobs;
aver_r=r_rfnW/nobs;
run;

%delet(&w_q_r&j);

/*������ȡÿ�ܵľ�ֵ��ϳ�һ�����ݼ������Ҫ����ƽ������Ŀǰ��û����*/
proc sql;
create table &w&j as
select distinct w_aver_q, w_aver_r, aver_q, aver_r
from &w_q_r&j;
quit;

%if &j=1 %then %do;
data C.Q_rovl_252_1;
set C.Q_rovl_252_1 &w&j;
run;
%end;

%if &j=2 %then %do;
data C.Q_rovl_252_2;
set C.Q_rovl_252_2 &w&j;
run;
%end;

%if &j=3 %then %do;
data C.Q_rovl_252_3;
set C.Q_rovl_252_3 &w&j;
run;
%end;

%if &j=4 %then %do;
data C.Q_rovl_252_4;
set C.Q_rovl_252_4 &w&j;
run;
%end;

%if &j=5 %then %do;
data C.Q_rovl_252_5;
set C.Q_rovl_252_5 &w&j;
run;
%end;


dm log 'clear;' continue;


proc datasets lib=work  nolist;
		delete &name1&j &data1&j &weight_q_r&j &w_q_r&j &w&j/ memtype=data;
		quit;



%end;
proc datasets lib=work  nolist;
		delete rank_test_&i / memtype=data;
		quit;

dm log 'clear;' continue;


%end;

%mend t_rovl;




%rank(29608);
