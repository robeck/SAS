/******************************************
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
t_...�����������Է���õ����ݽ��м��㣬ͳ�ƽ�����ص����ݼ���



*******************************************/



/***********************************
����rank����Ϊ����������Ҫʵ���ˣ�
1.��ȡ�����ÿһ��ȫ��ծȯ�����ݣ���������ݼ���
2.����split����������ʵ�ֶ������ݵķ��������
3.����t_..����������ʵ�ֶԼ�Ȩƽ��������ƽ���ļ���
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

%end;

dm log 'clear;' continue;

/*��Բ�ͬ���򣬵��ò�ͬ����*/
/*rank by rsj*/
%split(5,&M_week,rsj);
dm log 'clear;' continue;
%cal(5,&M_week,rsj);
dm log 'clear;' continue;

/*rank by rsk*/
%split(5,&M_week,rsk);
dm log 'clear;' continue;
%cal(5,&M_week,rsk);
dm log 'clear;' continue;

/*rank by rkt*/
%split(5,&M_week,rkt);
dm log 'clear;' continue;
%cal(5,&M_week,rkt);
dm log 'clear;' continue;

/*rank by rovl*/
%split(5,&M_week,rovl);
dm log 'clear;' continue;
%cal(5,&M_week,rovl);
dm log 'clear;' continue;


%mend rank;

/***************************
����
split����������Ҫʵ���ˣ�

****************************/
%macro split(k,h,v);


%do l=501 %to &h;

proc sort data=rank_&l;
by descending &v;
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
%macro cal(r,s,v);


%let t=1;
%let y=2;
%let g=3;
%let h=4;
%let b=5;
/*���ﴴ�������ݼ�Ϊ����5��ʱ*/
data C.Q_783_&v&t C.Q_783_&v&y C.Q_783_&v&g C.Q_783_&v&h C.Q_783_&v&b;
run;


%do i=501 %to &s;
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
data C.Q_783_&v&j;
set C.Q_783_&v&j &w&j;
run;
%end;

%if &j=2 %then %do;
data C.Q_783_&v&j;
set C.Q_783_&v&j &w&j;
run;
%end;

%if &j=3 %then %do;
data C.Q_783_&v&j;
set C.Q_783_&v&j &w&j;
run;
%end;

%if &j=4 %then %do;
data C.Q_783_&v&j;
set C.Q_783_&v&j &w&j;
run;
%end;

%if &j=5 %then %do;
data C.Q_783_&v&j;
set C.Q_783_&v&j &w&j;
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

%mend cal;






%rank(29608);
