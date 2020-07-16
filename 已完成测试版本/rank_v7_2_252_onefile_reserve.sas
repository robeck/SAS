/*

1.将按照各参数排序的qunity放入同一文件夹中，便于编程使用
2.保留residual参数，便于日后使用






*/


%macro rank(t);


%let M_week=252;



/*这里多个数据集合并的时候会造成内存崩溃，控制一次合并数据集的数量，保证运行顺利*/
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

/*V4新增*/


%end;

dm log 'clear;' continue;

/*根据排名进行分组 k为组数 h为选取的周数*/
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
/*对排序后的每个数据集 每个里面有t个债券，共5周的数据即h周，观测每周有多少个债券nobs然后根据k组拆分数据集*/
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


/*这里确保分配的债券向上取整，例如数据集有21个债券，分四组，那么每组取6个债券，最后一组为剩下的*/
%let n=%sysfunc(int(%sysevalf(&nobs/&k,ceil)));

%put &nobs &n &l;
dm log 'clear;' continue;
/*每个数据集有n=nobs/k个债券
  这一步对数据进行分组分配*/
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


/*这里确保分配的债券向上取整，例如数据集有21个债券，分四组，那么每组取6个债券，最后一组为剩下的*/
%let n=%sysfunc(int(%sysevalf(&nobs/&k,ceil)));

%put &nobs &n &l;
dm log 'clear;' continue;
/*每个数据集有n=nobs/k个债券
  这一步对数据进行分组分配*/
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


/*这里确保分配的债券向上取整，例如数据集有21个债券，分四组，那么每组取6个债券，最后一组为剩下的*/
%let n=%sysfunc(int(%sysevalf(&nobs/&k,ceil)));

%put &nobs &n &l;
dm log 'clear;' continue;
/*每个数据集有n=nobs/k个债券
  这一步对数据进行分组分配*/
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


/*这里确保分配的债券向上取整，例如数据集有21个债券，分四组，那么每组取6个债券，最后一组为剩下的*/
%let n=%sysfunc(int(%sysevalf(&nobs/&k,ceil)));

%put &nobs &n &l;
dm log 'clear;' continue;
/*每个数据集有n=nobs/k个债券
  这一步对数据进行分组分配*/
%sep(&k,&n,&l);
dm log 'clear;' continue;

%end;


%mend split_rovl;

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
%macro t_rsj(r,s);

/*这里创建的数据集为分组5组时*/
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

/*这里创建的数据集为分组5组时*/
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

/*这里创建的数据集为分组5组时*/
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

/*这里创建的数据集为分组5组时*/
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
