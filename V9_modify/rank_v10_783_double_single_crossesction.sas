/******************************************
v10 更新：
1.完成cross_section regression
2.rsj rkt rsk rovl参数已经实现，其他随后可加
****************************************

v9 更新：
1.实测double sorted是否可行。
2.完善阶段性代码结果
3.需要同时进行double single的排序进程
	a.针对single特性单独运行cal_single来实现
	b.single sorted必须和double区分开逻辑库，避免混乱
4.重新进行了降序排列法！！保证1-5为low-high
***********************************************

V8 更新：
1.进一步优化代码结构，删除了不必要的部分。
2.由于没有新的函数出现，命名上不做更改
****************************************************

V7 更新：
1.将按照各参数排序的qunity放入同一文件夹中，便于编程使用
2.保留residual参数，便于日后使用
******************************************************



程序简要说明：
1. 逻辑结构说明：
	a.根据统计出来的周化数据中最长周数783周为依据，我们首先按照周数来提取债券数据，例如；第一周，我们提取29608个债券中有第一周数据的债券，组成新的数据集rank_j。以此类推，直到783周，我们就可以获得至多783个数据集。
	b.对每一个rank_j我们执行拆分（split）操作，即我们需要对每一周的数据按照参数排序后分成5组qunities。
	c.对每组qunity中的数据统计bond excess return和Q值，计算加权平均和算数平均。

2. 主要函数块说明：
rank（）：提取每个债券在特定周中的数据，最终组合成数据集rank_j,随后执行split操作。
split（）：对rank_j按照特定参数排序，同时计算每一周中有多少债券，以确认划分的5组中需要包含多少数据。
	sep（）：用以完成split操作，划分qunities并且生成新数据集。
	delet（）：删除空白数据集，用来节约内存空间。
cal（）：用来对分组好的数据进行计算，统计结果到特点数据集。


!!ps:重要说明，此版本重新进行了升序排列法
*******************************************/



/***********************************
函数rank（）为主函数，主要实现了：
1.抽取，组合每一周全部债券的数据，组成周数据集。
2.调用split（）函数，实现对周数据的分组操作。
3.调用cal（）函数，实现对加权平均和算数平均的计算

t:数据集个数
************************************/
%macro rank(t);


%let M_week=783;



/*这里多个数据集合并的时候会造成内存崩溃，控制一次合并数据集的数量，保证运行顺利*/
%do j=501 %to &M_week;/*循环共M_week周*/
data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf Q rovl rkt rsj rsk resid_rsk_rsj resid_rsj_rsk id p_n);
set %do i=1 %to 5000;B.b_r_&i. %end;;/*提取i=1到5000个债券数据，以此类推*/
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

/*针对不同排序，调用不同函数*/
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
由于程序中只有针对不同参数的排序是不同的，因此这部分代码可以在程序中不断迭代，因此我们在这里使用参数v来迭代不同排序参数。
当rank中调用rsj，则这里的v就指代rsj参数。但rank中调用rsk，则这里的v就指代rsk参数。

split（）函数主要实现了：
1.对rank数据集按照特定参数v进行排序
2.计算每组qunity中有多少数据集n，便于对数据集进行进一步划分
3.调用sep（）函数，生成每组qunity的数据集

k:qunity个数
h:总共多少周
v:排序参数
****************************/
%macro split(k,h,v);


%do l=501 %to &h;

/*迭代的部分，每次调用不同参数v都会有不同排序结果*/
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


/*这里确保分配的债券向上取整，例如数据集有21个债券，分四组，那么每组取6个债券，最后一组为剩下的*/
%let n=%sysfunc(int(%sysevalf(&nobs/&k,ceil)));

%put &nobs &n &l;
dm log 'clear;' continue;
/*每个数据集有n=nobs/k个债券
  这一步对数据进行分组分配*/
%sep(&k,&n,&l);
dm log 'clear;' continue;

%end;


%mend split;


/*********************
sep（）函数主要实现了：
1.对rank数据集按照qunity组数进行划分，并按照n值确认每组有多少个债券。

k:qunity组数
n;每组最多有多少个债券
l:第l周
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



/*删除观测为0的数据集*/
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
split完成后，每周的数据集被分成了5份，代表5个qunity，即根据参数由大到小排列组成。例如数据集data11=第1周第1组qunity，data321=第32周第1组qunity。

cal（）函数主要实现了：
1.以前一周排序分组结果为基础，匹配当前周的债券数据，建立新的数据集weight_q_r&j。
2.在新的数据集中计算Q和bond excess return的加权与算数平均。
3.存入对应的数据集中，构成一个横截面均值数据集。

r:qunity的组数
s:周数
v:被调用的排序参数
***************************/
%macro cal(r,s,v,c);


/*这里创建的数据集为分组5*5组时*/
data C.w_11_783_&v C.w_12_783_&v C.w_13_783_&v C.w_14_783_&v C.w_15_783_&v C.w_21_783_&v C.w_22_783_&v C.w_23_783_&v C.w_24_783_&v C.w_25_783_&v C.w_31_783_&v C.w_32_783_&v C.w_33_783_&v
C.w_34_783_&v C.w_35_783_&v C.w_41_783_&v C.w_42_783_&v C.w_43_783_&v C.w_44_783_&v C.w_45_783_&v C.w_51_783_&v C.w_52_783_&v C.w_53_783_&v C.w_54_783_&v C.w_55_783_&v;
run;


%do i=501 %to &s;


%let data1=data_&i;
/*我们建立name_i数据集用来保存前一周的排序结果（仅保存债券id信息）*/
%let name1=name_&i;


%let f=%eval(&i+1);
%let weight_q_r=weight_q_r_&f;
%let w_q_r=w_q_r_&f;
%let w=w_&f;

%do j=1 %to &r;

/*由于我们根据前一周的排序来计算后一周的，所以我们取前一周的id信息*/
data &name1&j;
set &data1&j (keep=cusip_id);
run;

/*匹配前一周的id信息与当前周的数据，生成新数据集，并用于后期运算*/
proc sql;
create table &weight_q_r&j as
select * from &name1&j as a left join rank_&f as b on a.cusip_id=b.cusip_id;
quit;

proc sort data=&weight_q_r&j;
by &c;
run;

/*删除无用数据集节约空间*/
proc datasets lib=work  nolist;
delete &name1&j &data1&j/memtype=data;
quit;
/*
对Quintus i中的债券再分组
weight_q_r_ij表示i周第j个quintiles
随后我们是由split_new对第j个quintiles进行进一步切割，分成五组quintiles：为wsep_ijm,其中m为新的quintile

因此对我们要计算的是m个qunitiles
且我们保存到的数据集必须是二维
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


/*这里确保分配的债券向上取整，例如数据集有21个债券，分四组，那么每组取6个债券，最后一组为剩下的*/
%let n=%sysfunc(int(%sysevalf(&nobs/&k,ceil)));

%put &nobs &n;

/*每个数据集有n=nobs/k个债券
  这一步对数据进行分组分配*/
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

/*根据权重，我们计算埃尔夫的组内均值 和 return的组内均值*/

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

/*我们提取每周的均值组合成一个数据集，最后还要计算平均，但目前还没计算*/
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

/*删除无用数据集节约空间*/
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
/*这里创建的数据集为分组5组时*/
data D.Q_783_&v&t D.Q_783_&v&y D.Q_783_&v&g D.Q_783_&v&h D.Q_783_&v&b;
run;


%do i=501 %to &s;


%let data1=data_&i;
/*我们建立name_i数据集用来保存前一周的排序结果（仅保存债券id信息）*/
%let name1=name_&i;


%let f=%eval(&i+1);
%let weight_q_r=weight_q_r_&f;
%let w_q_r=w_q_r_&f;
%let w=w_&f;

%do j=1 %to &r;

/*由于我们根据前一周的排序来计算后一周的，所以我们取前一周的id信息*/
data &name1&j;
set &data1&j (keep=cusip_id);
run;

/*匹配前一周的id信息与当前周的数据，生成新数据集，并用于后期运算*/
proc sql;
create table &weight_q_r&j as
select *,sum(p_n) as total from &name1&j as a left join rank_&f as b on a.cusip_id=b.cusip_id;

quit;


/*计算权重，随后再计算每周的Q和bond excess return的加权平均与算数平均*/
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

/*我们提取每周的均值组合成一个数据集*/
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

/*删除无用数据集节约空间*/
proc datasets lib=work  nolist;
		delete &name1&j &data1&j &weight_q_r&j &w_q_r&j &w&j/ memtype=data;
		quit;



%end;

/*删除无用数据集节约空间*/
proc datasets lib=work  nolist;
		delete rank_test_&i / memtype=data;
		quit;

dm log 'clear;' continue;


%end;

%mend cal_single;


/*****************************************
r_reg()函数实现了：
1.对cross_section的回归

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

/*迭代的部分，每次调用不同参数v都会有不同排序结果*/
proc sort data=rank_&l;
by &v;
run;

data rank_&l;
set rank_&l;
if r_rf='.' then delete;
run;


/*每个数据集有n=nobs/k个债券
  这一步对数据进行分组分配*/
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
