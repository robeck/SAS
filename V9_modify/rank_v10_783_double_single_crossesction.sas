/******************************************
v10 ���£�
1.���cross_section regression
2.rsj rkt rsk rovl�����Ѿ�ʵ�֣��������ɼ�
****************************************

v9 ���£�
1.ʵ��double sorted�Ƿ���С�
2.���ƽ׶��Դ�����
3.��Ҫͬʱ����double single���������
	a.���single���Ե�������cal_single��ʵ��
	b.single sorted�����double���ֿ��߼��⣬�������
4.���½����˽������з�������֤1-5Ϊlow-high
***********************************************

V8 ���£�
1.��һ���Ż�����ṹ��ɾ���˲���Ҫ�Ĳ��֡�
2.����û���µĺ������֣������ϲ�������
****************************************************

V7 ���£�
1.�����ո����������qunity����ͬһ�ļ����У����ڱ��ʹ��
2.����residual�����������պ�ʹ��
******************************************************



�����Ҫ˵����
1. �߼��ṹ˵����
	a.����ͳ�Ƴ������ܻ������������783��Ϊ���ݣ��������Ȱ�����������ȡծȯ���ݣ����磻��һ�ܣ�������ȡ29608��ծȯ���е�һ�����ݵ�ծȯ������µ����ݼ�rank_j���Դ����ƣ�ֱ��783�ܣ����ǾͿ��Ի������783�����ݼ���
	b.��ÿһ��rank_j����ִ�в�֣�split����������������Ҫ��ÿһ�ܵ����ݰ��ղ��������ֳ�5��qunities��
	c.��ÿ��qunity�е�����ͳ��bond excess return��Qֵ�������Ȩƽ��������ƽ����

2. ��Ҫ������˵����
rank��������ȡÿ��ծȯ���ض����е����ݣ�������ϳ����ݼ�rank_j,���ִ��split������
split��������rank_j�����ض���������ͬʱ����ÿһ�����ж���ծȯ����ȷ�ϻ��ֵ�5������Ҫ�����������ݡ�
	sep�������������split����������qunities�������������ݼ���
	delet������ɾ���հ����ݼ���������Լ�ڴ�ռ䡣
cal�����������Է���õ����ݽ��м��㣬ͳ�ƽ�����ص����ݼ���


!!ps:��Ҫ˵�����˰汾���½������������з�
*******************************************/



/***********************************
����rank����Ϊ����������Ҫʵ���ˣ�
1.��ȡ�����ÿһ��ȫ��ծȯ�����ݣ���������ݼ���
2.����split����������ʵ�ֶ������ݵķ��������
3.����cal����������ʵ�ֶԼ�Ȩƽ��������ƽ���ļ���

t:���ݼ�����
************************************/
%macro rank(t);


%let M_week=783;



/*���������ݼ��ϲ���ʱ�������ڴ����������һ�κϲ����ݼ�����������֤����˳��*/
%do j=501 %to &M_week;/*ѭ����M_week��*/
data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk resid_rsk_rsj resid_rsj_rsk id p_n);
set %do i=1 %to 5000;B.b_r_&i. %end;;/*��ȡi=1��5000��ծȯ���ݣ��Դ�����*/
where id=&j;
run;

dm log 'clear;' continue;


data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk resid_rsk_rsj resid_rsj_rsk id p_n);
set rank_&j %do i=5001 %to 10000;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;


data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk resid_rsk_rsj resid_rsj_rsk id p_n);
set rank_&j %do i=10001 %to 15000;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;

data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk resid_rsk_rsj resid_rsj_rsk id p_n);
set rank_&j %do i=15001 %to 20000;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;

data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk resid_rsk_rsj resid_rsj_rsk id p_n);
set rank_&j %do i=20001 %to 25000;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;

data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk resid_rsk_rsj resid_rsj_rsk id p_n);
set rank_&j %do i=25001 %to 29608;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;

%end;


dm log 'clear;' continue;

/*��Բ�ͬ���򣬵��ò�ͬ����*/
/*rank by rsj*/
%split(5,&M_week,rsj);
dm log 'clear;' continue;
%cal(5,&M_week,rsj,rsk);
dm log 'clear;' continue;
%split(5,&M_week,rsj);
dm log 'clear;' continue;
%cal_single(5,&M_week,rsj);
dm log 'clear;' continue;
%r_reg(5,&M_week,rsj);
dm log 'clear;' continue;


%split(5,&M_week,rsk);
dm log 'clear;' continue;
%cal(5,&M_week,rsk,rsj);
dm log 'clear;' continue;
%split(5,&M_week,rsk);
dm log 'clear;' continue;
%cal_single(5,&M_week,rsk);
dm log 'clear;' continue;
%r_reg(5,&M_week,rsk);
dm log 'clear;' continue;


%split(5,&M_week,rkt);
dm log 'clear;' continue;
%cal_single(5,&M_week,rkt);
dm log 'clear;' continue;
%r_reg(5,&M_week,rkt);
dm log 'clear;' continue;


%split(5,&M_week,rovl);
dm log 'clear;' continue;
%cal_single(5,&M_week,rovl);
dm log 'clear;' continue;
%r_reg(5,&M_week,rovl);
dm log 'clear;' continue;


%split(5,&M_week,resid_rsj_rsk);
dm log 'clear;' continue;
%cal_single(5,&M_week,resid_rsj_rsk);
dm log 'clear;' continue;
%r_reg(5,&M_week,resid_rsj_rsk);
dm log 'clear;' continue;


%split(5,&M_week,resid_rsk_rsj);
dm log 'clear;' continue;
%cal_single(5,&M_week,resid_rsk_rsj);
dm log 'clear;' continue;
%r_reg(5,&M_week,resid_rsk_rsj);
dm log 'clear;' continue;



%mend rank;

/***************************
���ڳ�����ֻ����Բ�ͬ�����������ǲ�ͬ�ģ�����ⲿ�ִ�������ڳ����в��ϵ������������������ʹ�ò���v��������ͬ���������
��rank�е���rsj���������v��ָ��rsj��������rank�е���rsk���������v��ָ��rsk������

split����������Ҫʵ���ˣ�
1.��rank���ݼ������ض�����v��������
2.����ÿ��qunity���ж������ݼ�n�����ڶ����ݼ����н�һ������
3.����sep��������������ÿ��qunity�����ݼ�

k:qunity����
h:�ܹ�������
v:�������
****************************/
%macro split(k,h,v);


%do l=501 %to &h;

/*�����Ĳ��֣�ÿ�ε��ò�ͬ����v�����в�ͬ������*/
proc sort data=rank_&l;
by &v;
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


%mend split;


/*********************
sep����������Ҫʵ���ˣ�
1.��rank���ݼ�����qunity�������л��֣�������nֵȷ��ÿ���ж��ٸ�ծȯ��

k:qunity����
n;ÿ������ж��ٸ�ծȯ
l:��l��
********************/
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


/**************************
split��ɺ�ÿ�ܵ����ݼ����ֳ���5�ݣ�����5��qunity�������ݲ����ɴ�С������ɡ��������ݼ�data11=��1�ܵ�1��qunity��data321=��32�ܵ�1��qunity��

cal����������Ҫʵ���ˣ�
1.��ǰһ�����������Ϊ������ƥ�䵱ǰ�ܵ�ծȯ���ݣ������µ����ݼ�weight_q_r&j��
2.���µ����ݼ��м���Q��bond excess return�ļ�Ȩ������ƽ����
3.�����Ӧ�����ݼ��У�����һ��������ֵ���ݼ���

r:qunity������
s:����
v:�����õ��������
***************************/
%macro cal(r,s,v,c);


/*���ﴴ�������ݼ�Ϊ����5*5��ʱ*/
data C.w_11_783_&v C.w_12_783_&v C.w_13_783_&v C.w_14_783_&v C.w_15_783_&v C.w_21_783_&v C.w_22_783_&v C.w_23_783_&v C.w_24_783_&v C.w_25_783_&v C.w_31_783_&v C.w_32_783_&v C.w_33_783_&v
C.w_34_783_&v C.w_35_783_&v C.w_41_783_&v C.w_42_783_&v C.w_43_783_&v C.w_44_783_&v C.w_45_783_&v C.w_51_783_&v C.w_52_783_&v C.w_53_783_&v C.w_54_783_&v C.w_55_783_&v;
run;


%do i=501 %to &s;


%let data1=data_&i;
/*���ǽ���name_i���ݼ���������ǰһ�ܵ���������������ծȯid��Ϣ��*/
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

/*ƥ��ǰһ�ܵ�id��Ϣ�뵱ǰ�ܵ����ݣ����������ݼ��������ں�������*/
proc sql;
create table &weight_q_r&j as
select * from &name1&j as a left join rank_&f as b on a.cusip_id=b.cusip_id;
quit;

proc sort data=&weight_q_r&j;
by &c;
run;

/*ɾ���������ݼ���Լ�ռ�*/
proc datasets lib=work  nolist;
delete &name1&j &data1&j/memtype=data;
quit;
/*
��Quintus i�е�ծȯ�ٷ���
weight_q_r_ij��ʾi�ܵ�j��quintiles
�����������split_new�Ե�j��quintiles���н�һ���и�ֳ�����quintiles��Ϊwsep_ijm,����mΪ�µ�quintile

��˶�����Ҫ�������m��qunitiles
�����Ǳ��浽�����ݼ������Ƕ�ά
*/
%split_new(&weight_q_r&j,&r,&f,&j);

%end;


%end;

%mend cal;

%macro split_new(ds,k,f,j);


data _null_;
set &ds end=eof nobs=count;
if eof then call symput('nobs', left(count));
run;


/*����ȷ�������ծȯ����ȡ�����������ݼ���21��ծȯ�������飬��ôÿ��ȡ6��ծȯ�����һ��Ϊʣ�µ�*/
%let n=%sysfunc(int(%sysevalf(&nobs/&k,ceil)));

%put &nobs &n;

/*ÿ�����ݼ���n=nobs/k��ծȯ
  ��һ�������ݽ��з������*/
%sep_new(&ds,&n,&k,&f,&j);


%mend split_new;

%macro sep_new(ds,n,k,f,j);


%let wsep=wsep_&f&j;
%let w_sep=w_sep_&f&j;

%let w_q_r=w_q_r_&f&j;
%let w=w_&f&j;


%do m=1 %to &k;
data &wsep&m &ds;
set &ds;
if _N_<=&n then output &wsep&m;
else output &ds;
%delet(&ds);
run;

proc sql;
create table &w_sep&m as
seletc *,sum(p_n) as total
from &wsep&m;
quit;

/*����Ȩ�أ����Ǽ��㰣��������ھ�ֵ �� return�����ھ�ֵ*/

data &w_sep&m;
set &w_sep&m;
if Q=' ' then delete;
weight=(p_n)/(total);
q_w=Q*weight;
r_rf_w=r_rf*weight;
run;

%delet(&wsep&m);
%delet(&w_sep&m);

proc sql;
create table &w_q_r&m as
select *,sum(q_w) as QW,sum(r_rf_w) as RRFW,sum(Q) as QnW,sum(r_rf) as r_rfnW
from &w_sep&m;
quit;


data &w_q_r&m;
set &w_q_r&m nobs=nobs;
w_aver_q=QW;
w_aver_r=RRFW;
aver_q=QnW/nobs;
aver_r=r_rfnW/nobs;
run;

%delet(&w_q_r&m);

/*������ȡÿ�ܵľ�ֵ��ϳ�һ�����ݼ������Ҫ����ƽ������Ŀǰ��û����*/
proc sql;
create table &w&m as
select distinct w_aver_q, w_aver_r, aver_q, aver_r
from &w_q_r&m;
quit;

%if &j=1 %then %do;

	%if &m=1 %then %do;
	data C.w_11_783_&v;
	set C.w_11_783_&v &w&m;
	run;
	%end;

	%if &m=2 %then %do;
	data C.w_12_783_&v;
	set C.w_12_783_&v &w&m;
	run;
	%end;

	%if &m=3 %then %do;
	data C.w_13_783_&v;
	set C.w_13_783_&v &w&m;
	run;
	%end;

	%if &m=4 %then %do;
	data C.w_14_783_&v;
	set C.w_14_783_&v &w&m;
	run;
	%end;

	%if &m=5 %then %do;
	data C.w_15_783_&v;
	set C.w_15_783_&v &w&m;
	run;
	%end;

%end;

%if &j=2 %then %do;


	%if &m=1 %then %do;
	data C.w_21_783_&v;
	set C.w_21_783_&v &w&m;
	run;
	%end;

	%if &m=2 %then %do;
	data C.w_22_783_&v;
	set C.w_22_783_&v &w&m;
	run;
	%end;

	%if &m=3 %then %do;
	data C.w_23_783_&v;
	set C.w_23_783_&v &w&m;
	run;
	%end;

	%if &m=4 %then %do;
	data C.w_24_783_&v;
	set C.w_24_783_&v &w&m;
	run;
	%end;

	%if &m=5 %then %do;
	data C.w_25_783_&v;
	set C.w_25_783_&v &w&m;
	run;
	%end;


%end;

%if &j=3 %then %do;


	%if &m=1 %then %do;
	data C.w_31_783_&v;
	set C.w_31_783_&v &w&m;
	run;
	%end;

	%if &m=2 %then %do;
	data C.w_32_783_&v;
	set C.w_32_783_&v &w&m;
	run;
	%end;

	%if &m=3 %then %do;
	data C.w_33_783_&v;
	set C.w_33_783_&v &w&m;
	run;
	%end;

	%if &m=4 %then %do;
	data C.w_34_783_&v;
	set C.w_34_783_&v &w&m;
	run;
	%end;

	%if &m=5 %then %do;
	data C.w_35_783_&v;
	set C.w_35_783_&v &w&m;
	run;
	%end;

%end;

%if &j=4 %then %do;

%if &m=1 %then %do;
	data C.w_41_783_&v;
	set C.w_41_783_&v &w&m;
	run;
	%end;

	%if &m=2 %then %do;
	data C.w_42_783_&v;
	set C.w_42_783_&v &w&m;
	run;
	%end;

	%if &m=3 %then %do;
	data C.w_43_783_&v;
	set C.w_43_783_&v &w&m;
	run;
	%end;

	%if &m=4 %then %do;
	data C.w_44_783_&v;
	set C.w_44_783_&v &w&m;
	run;
	%end;

	%if &m=5 %then %do;
	data C.w_45_783_&v;
	set C.w_45_783_&v &w&m;
	run;
	%end;


%end;

%if &j=5 %then %do;

%if &m=1 %then %do;
	data C.w_51_783_&v;
	set C.w_51_783_&v &w&m;
	run;
	%end;

	%if &m=2 %then %do;
	data C.w_52_783_&v;
	set C.w_52_783_&v &w&m;
	run;
	%end;

	%if &m=3 %then %do;
	data C.w_53_783_&v;
	set C.w_53_783_&v &w&m;
	run;
	%end;

	%if &m=4 %then %do;
	data C.w_54_783_&v;
	set C.w_54_783_&v &w&m;
	run;
	%end;

	%if &m=5 %then %do;
	data C.w_55_783_&v;
	set C.w_55_783_&v &w&m;
	run;
	%end;

%end;

/*ɾ���������ݼ���Լ�ռ�*/
proc datasets lib=work  nolist;
		delete &wsep&m &w_sep&m &w_q_r&m &w&m/ memtype=data;
		quit;
%end;

proc datasets lib=work  nolist;
		delete &ds / memtype=data;
		quit;



%mend sep_new();


%macro cal_single(r,s,v);

%let t=1;
%let y=2;
%let g=3;
%let h=4;
%let b=5;
/*���ﴴ�������ݼ�Ϊ����5��ʱ*/
data D.Q_783_&v&t D.Q_783_&v&y D.Q_783_&v&g D.Q_783_&v&h D.Q_783_&v&b;
run;


%do i=501 %to &s;


%let data1=data_&i;
/*���ǽ���name_i���ݼ���������ǰһ�ܵ���������������ծȯid��Ϣ��*/
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

/*ƥ��ǰһ�ܵ�id��Ϣ�뵱ǰ�ܵ����ݣ����������ݼ��������ں�������*/
proc sql;
create table &weight_q_r&j as
select *,sum(p_n) as total from &name1&j as a left join rank_&f as b on a.cusip_id=b.cusip_id;

quit;


/*����Ȩ�أ�����ټ���ÿ�ܵ�Q��bond excess return�ļ�Ȩƽ��������ƽ��*/
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
w_aver_q=QW;
w_aver_r=RRFW;
aver_q=QnW/nobs;
aver_r=r_rfnW/nobs;
run;

%delet(&w_q_r&j);

/*������ȡÿ�ܵľ�ֵ��ϳ�һ�����ݼ�*/
proc sql;
create table &w&j as
select distinct w_aver_q, w_aver_r, aver_q, aver_r
from &w_q_r&j;
quit;

%if &j=1 %then %do;
data D.Q_783_&v&j;
set D.Q_783_&v&j &w&j;
run;
%end;

%if &j=2 %then %do;
data D.Q_783_&v&j;
set D.Q_783_&v&j &w&j;
run;
%end;

%if &j=3 %then %do;
data D.Q_783_&v&j;
set D.Q_783_&v&j &w&j;
run;
%end;

%if &j=4 %then %do;
data D.Q_783_&v&j;
set D.Q_783_&v&j &w&j;
run;
%end;

%if &j=5 %then %do;
data D.Q_783_&v&j;
set D.Q_783_&v&j &w&j;
run;
%end;


dm log 'clear;' continue;

/*ɾ���������ݼ���Լ�ռ�*/
proc datasets lib=work  nolist;
		delete &name1&j &data1&j &weight_q_r&j &w_q_r&j &w&j/ memtype=data;
		quit;



%end;

/*ɾ���������ݼ���Լ�ռ�*/
proc datasets lib=work  nolist;
		delete rank_test_&i / memtype=data;
		quit;

dm log 'clear;' continue;


%end;

%mend cal_single;


/*****************************************
r_reg()����ʵ���ˣ�
1.��cross_section�Ļع�

*****************************************/
%macro r_reg(k,h,v);

data E.reg_multiple_783;
run;

data E.reg_rsj_783;
run;

data E.reg_rsk_783;
run;

data E.reg_rkt_783;
run;

data E.reg_rovl_783;
run;

%do l=501 %to &h;

/*�����Ĳ��֣�ÿ�ε��ò�ͬ����v�����в�ͬ������*/
proc sort data=rank_&l;
by &v;
run;

data rank_&l;
set rank_&l;
if r_rf='.' then delete;
run;


/*ÿ�����ݼ���n=nobs/k��ծȯ
  ��һ�������ݽ��з������*/
%reg(&l);


%end;


%mend r_reg;

%macro reg(i);


proc reg data=rank_&i outest=est noprint;
model r_rf=rsj rsk rkt rovl;
quit;


data E.reg_multiple_783(keep=rsj rkt rsk rovl);
set E.reg_multiple_783 est;
run;

/*single rsj*/
proc reg data=rank_&i outest=estrsj noprint;
model r_rf=rsj;
quit;

data E.reg_rsj_783(keep=rsj);
set E.reg_rsj_783 estrsj;
run;

/*single rsk*/
proc reg data=rank_&i outest=estrsk noprint;
model r_rf=rsk;
quit;

data E.reg_rsk_783(keep=rsk);
set E.reg_rsk_783 estrsk;
run;

/*single rkt*/
proc reg data=rank_&i outest=estrkt noprint;
model r_rf=rkt;
quit;

data E.reg_rkt_783(keep=rkt);
set E.reg_rkt_783 estrkt;
run;

/*single rovl*/
proc reg data=rank_&i outest=estrovl noprint;
model r_rf=rovl;
quit;

data E.reg_rovl_783(keep=rovl);
set E.reg_rovl_783 estrovl;
run;




%mend reg;



%rank(29608);
