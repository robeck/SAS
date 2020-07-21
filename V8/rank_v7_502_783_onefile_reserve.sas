/******************************************
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
t_...（）：用来对分组好的数据进行计算，统计结果到特点数据集。



*******************************************/



/***********************************
函数rank（）为主函数，主要实现了：
1.抽取，组合每一周全部债券的数据，组成周数据集。
2.调用split（）函数，实现对周数据的分组操作。
3.调用t_..（）函数，实现对加权平均和算数平均的计算
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

/*针对不同排序，调用不同函数*/
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
由于
split（）函数主要实现了：

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


/*k为组数，n为每组所包含的债券，l为当前周
因此每周排序好的数据集会被拆分出来，共k组*/
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

/*split完成后，我们得到每周，分成5份的数据集*/
%macro cal(r,s,v);


%let t=1;
%let y=2;
%let g=3;
%let h=4;
%let b=5;
/*这里创建的数据集为分组5组时*/
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
/*由于我们根据前一周的排序来计算后一周的，所以我们取前一周的id信息*/
data &name1&j;
set &data1&j (keep=cusip_id);
run;

/*匹配前一周的id信息与当前周的数据*/
proc sql;
create table &weight_q_r&j as
select *,sum(p_n) as total from &name1&j as a left join rank_&f as b on a.cusip_id=b.cusip_id;

quit;


/*根据权重，我们计算埃尔夫的组内均值 和 return的组内均值*/

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

/*我们提取每周的均值组合成一个数据集，最后还要计算平均，但目前还没计算*/
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
