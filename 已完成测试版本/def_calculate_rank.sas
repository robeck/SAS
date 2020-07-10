/*def因子的计算*/
%macro split(ds,ds1,ds2);
      
        proc sql noprint;
        select distinct cusip_id into: names separated by ','/*所有类别放入宏names，逗号分隔*/
        from &ds;
        quit;
        %let i=1;
        %do %while(%scan(%quote(&names),&i,',') ne %str());/*子串不为空是循环拆分数据集*/
                %let dname=%scan(%quote(&names),&i,',');
				
				
				data D_&i;
                set &ds2;
                where cusip_id = "&dname";
                run;
				
				proc sql noprint;
				create table Ds_&i as
				select * from D_&i as a left join A.name_info_new as b on a.cusip_id=b.cusip_id; /*name_info_new 其中的coupon为重新计算获得*/
				quit;

				/*原始数据已经按照要求排序*/
				proc sort data=Ds_&i;
				by trd_exctn_dt trd_exctn_tm;
				run;

				data Ds_&i;
				set Ds_&i;
				id=_N_;
				run;

				proc datasets lib=work  nolist;
				delete D_&i / memtype=data;
				quit;

				%loop(&i);
				
	
                %let i=%eval(&i.+1);
				
				 dm log 'clear;' continue; 
		

        %end;



		proc sql noprint;
        select distinct cusip_id into: namess separated by ','/*所有类别放入宏names，逗号分隔*/
        from &ds1;
        quit;
        %let j=1;
        %let o=6000;
        %do %while(%scan(%quote(&namess),&j,',') ne %str());/*子串不为空是循环拆分数据集*/
                %let dnames=%scan(%quote(&namess),&j,',');
				
				
				data D_&o;
                set &ds2;
                where cusip_id = "&dnames";
                run;

				proc sql noprint;
				create table Ds_&o as
				select * from D_&o as a left join A.name_info_new as b on a.cusip_id=b.cusip_id;
				quit;

				/*原始数据已经按照要求排序*/
				proc sort data=Ds_&o;
				by trd_exctn_dt trd_exctn_tm;
				run;

				data Ds_&o;
				set Ds_&o;
				id=_N_;
				run;

				proc datasets lib=work  nolist;
				delete D_&o / memtype=data;
				quit;

				%loop(&o);
				
				%let o=%eval(&o.+1);
                %let j=%eval(&j.+1);
				
				
		 dm log 'clear;' continue; 

        %end;

		%def_rank(11263);

		
		



		
%mend split;


%macro loop(i);

 %bond_return(Ds_&i,&i);



 dm log 'clear;' continue;

%mend loop;

%macro bond_return(ds,i);

/*计算每一天的平均价格price 和总成交量quantity 用于在数据补全时候的准确性*/
/*问题：补全的数据没有问题，但计算的每日均值并非我们要找的每周最后一笔交易，因此我们补全数据后，需要和原始数据融合，保持原始数据的所有日期中最后一笔交易*/
data &ds;
set &ds;
total=rptd_pr*entrd_vol_qt;
run;

proc sql;
create table test_&i as
select *, sum(entrd_vol_qt) as total_quan, sum(total) as day_total
from &ds
group by trd_exctn_dt;
quit;

data br_1_&i;
set test_&i;
pri=round((day_total/total_quan),0.001);

data br_1_&i;
set br_1_&i(drop=rptd_pr entrd_vol_qt day_total total rename=(total_quan=entrd_vol_qt) rename=(pri=rptd_pr));
label rptd_pr='Price';
label entrd_vol_qt='quantity';
run;


proc sql;
create table br_2_&i as
select  * from br_1_&i
where id in (select max(id) from br_1_&i group by trd_exctn_dt);
quit;


/*V6 对bond return也做数据真实性补充*/
data br_2_test1_&i(rename=(_date=trd_exctn_dt) rename=(_price=rptd_pr) rename=(_quantity=entrd_vol_qt));
   set br_2_&i(drop=id);
	by cusip_id;
   retain  _date count_day _price _quantity;
   format _date YYMMDDN8.;
   if first.cusip_id then do;
      count_day=1;
          _date=trd_exctn_dt;
		  _price=rptd_pr;
		  _quantity=entrd_vol_qt;
          output;
    end;
     else do;
	 		   
			   
               _date=intnx('day',_date,1);
                  if _date<trd_exctn_dt then do until (_date=trd_exctn_dt);
              count_day=count_day;
			     _price=rptd_pr;
		 		 _quantity=entrd_vol_qt;
               output;
              _date=intnx('day',_date,1);
                       end;
                 if _date=trd_exctn_dt then do;
                                count_day+1;
								_price=rptd_pr;
								_quantity=entrd_vol_qt;
                                output;
                             end;
     end;
drop trd_exctn_dt;
drop rptd_pr;
drop entrd_vol_qt;
run;

data br_2_test1_&i;
set br_2_test1_&i;
wd=WEEKDAY(trd_exctn_dt);
if _n_=1 then do;
if wd>3 then call symput('e',7-(wd-3));
if wd<3 then call symput('e',3-wd);
if wd=3 then call symput('e',0);
end;
run;

/*不计算周末，删除周末*/
data br_2_test1_&i;
set br_2_test1_&i;
%put &e;
if _N_<=&e then delete;
if wd=6 or wd=7 then delete;
run;

data br_2_test1_&i;
set br_2_test1_&i;
rename trd_exctn_dt=trade_date;
rename rptd_pr=price;
run;


data br_2_test1_&i(keep=cusip_id trade_date price);
set br_2_test1_&i;
run;

data br_2_test1_&i;
set br_2_test1_&i;
t=mod(_n_,5);
if t=0 then t=5;

retain pr5;
if t=1 then do;
pr5=price;
end;
else do;
pr5=pr5+price;
end;
if t=5 then output;
run;


data br_2_test2_&i;
set br_2_test1_&i;
pr=pr5/5;
drop pr5 price t;
run;



/*v4新增部分*/
/*这里增加了个参数，可以计算缺失数据的相隔天*/
data br_3_&i(drop= id rename=(_date=trd_exctn_dt) rename=(_price=rptd_pr)rename=(_quantity=entrd_vol_qt));
   set br_2_&i;
	by cusip_id;
   retain  _date count_day _price _quantity;
   format _date YYMMDDN8.;
   if first.cusip_id then do;
      count_day=0;
	  count_day_a=1;
          _date=trd_exctn_dt;
		  _price=rptd_pr;
		  _quantity=entrd_vol_qt;
          output;
    end;
     else do;
	 		   
			   
               _date=intnx('day',_date,1);
                  if _date<trd_exctn_dt then do until (_date=trd_exctn_dt);
              count_day=count_day+1;
			  _price='.';
			  _quantity='.';
               output;
              _date=intnx('day',_date,1);
                       end;
                 if _date=trd_exctn_dt then do;
                                count_day_a+1;
								count_day=0;
								_price=rptd_pr;
								_quantity=entrd_vol_qt;
                                output;
                             end;
     end;
drop trd_exctn_dt;
drop rptd_pr;
drop entrd_vol_qt;
run;


/*以每周三开始计算一周*/
data br_3_&i;
set br_3_&i;
wd=WEEKDAY(trd_exctn_dt);
if _n_=1 then do;
if wd>3 then call symput('e',7-(wd-3));
if wd<3 then call symput('e',3-wd);
if wd=3 then call symput('e',0);
end;
run;

/*不计算周末，删除周末*/
data br_3_&i;
set br_3_&i;
%put &e;
if _N_<=&e then delete;
if wd=6 or wd=7 then delete;
run;


/*该处的三步为了处理统计缺失的天，标记为1*/
data br_3_&i(drop=count_day count_day_a);
set br_3_&i;
if rptd_pr=0 then rptd_pr='.';
if entrd_vol_qt=0 then entrd_vol_qt='.';
run;


data br_3_&i;
set br_3_&i;
by cusip_id;
retain count_t;
 if first.cusip_id then do;
      count_t=0;
	  output;
	  end;
	else do;
	  if rptd_pr='.' then do;
	  count_t=1;
    output;
	end;
		if rptd_pr^='.' then do;
		count_t=0;
		output;
		end;
end;

run;

data br_3_&i;
set br_3_&i;
if rptd_pr='.' then rptd_pr=0;
if entrd_vol_qt='.' then entrd_vol_qt=0;
run;

/*取五天为一个周期，并且计算这一周中有几天缺失数据统计为count_5*/
data br_3_&i(keep=cusip_id trd_exctn_dt coupon interest_frequency first_interest_date rptd_pr_5 entrd_vol_qt_5 count_5);
set br_3_&i;
t=mod(_n_,5);
if t=0 then t=5;

retain rptd_pr_5 entrd_vol_qt_5 count_5;
if t=1 then do;
rptd_pr_5=rptd_pr;
entrd_vol_qt_5=entrd_vol_qt;
count_5=count_t;
end;
else do;
rptd_pr_5=rptd_pr_5+rptd_pr;
entrd_vol_qt_5=entrd_vol_qt_5+entrd_vol_qt;
count_5=count_5+count_t;
end;
if t=5 then output;

run;

/*重新计算count_5为有交易的天数，然后计算每周的平均做为该周最后一笔交易，记为rptd_pr_5。。。*/
data br_4_&i;
set br_3_&i;
count_5=5-count_5;

if rptd_pr_5^=0 then do;
rptd_pr_5=rptd_pr_5/count_5;
entrd_vol_qt_5=entrd_vol_qt_5/count_5;
end;
run;

/*该处的四步为了填补缺失仅一周的数据，即三周中仅有中间一周缺失的情况，这种情况做除数据填补，方法为前后两周数据的平均*/
data br_4_1_&i(keep=count_5 pr qt id);
set br_4_&i;
pr=lag(rptd_pr_5);
qt=lag(entrd_vol_qt_5);
id=_n_;
run;

data br_4_2_&i;
set br_4_&i;
if _n_=1 then delete;
pr2=rptd_pr_5;
qt2=entrd_vol_qt_5;
run;

data br_4_2_&i(keep=count_5 pr2 qt2 id);
set br_4_2_&i;
id=_n_;
run;

data br_4_3_&i;
merge br_4_1_&i br_4_2_&i;
by id;
p=(pr+pr2)/2;
q=(qt+qt2)/2;
run;

data br_4_&i;
set br_4_&i;
id=_n_;
run;

data br_5_&i;
merge br_4_&i br_4_3_&i;
by id;
run;
 
/*计算要填补的周的数据*/
data br_5_&i;
set br_5_&i;
if rptd_pr_5=0 and pr^=0 and pr2^=0 then do;
rptd_pr_5=p;
end;

if entrd_vol_qt_5=0 and qt^=0 and qt2^=0 then do;
entrd_vol_qt_5=q;
end;

run;

data br_5_&i(rename=(entrd_vol_qt_5=entrd_vol_qt) rename=(rptd_pr_5=rptd_pr) drop=pr pr2 qt qt2 count_5);
set br_5_&i;
run;
/*v4新增部分结束*/

/*v6*/
proc sql noprint;
create table br_5_test1_&i as
select *
from br_5_&i as a left join br_2_test2_&i as b on a.trd_exctn_dt-b.trade_date=0 or a.trd_exctn_dt-b.trade_date=1 or a.trd_exctn_dt-b.trade_date=2 or a.trd_exctn_dt-b.trade_date=3 or a.trd_exctn_dt-b.trade_date=4 or a.trd_exctn_dt-b.trade_date=5 or a.trd_exctn_dt-b.trade_date=6;
quit;


data br_5_test1_&i;
set br_5_test1_&i;
if rptd_pr=0 then do;
rptd_pr=pr;
end;

drop p q trade_date pr;
run;


/*dm 计算了从第一笔交易开始，到首次付息日所差的天数
 dw 计算了这些天数所在的周，abs取绝对值，用于计算准确交割周*/
data br_5_test1_&i;
set br_5_test1_&i nobs=nobs;
d_m=first_interest_date-trd_exctn_dt;
d_w=ceil(round(d_m/7,0.001));
d_w=abs(d_w);
run;




/*用来计算AIT */
%interests(br_5_test1_&i,&i);

/*用来计算coupon，非交割日期时候的coupon均为0*/
data br_5_test1_&i(drop=p q);
set br_5_test1_&i;
if A=0 then cou=coup;
else if A^=0 then cou=0;
e=rptd_pr+A; /* e 为pit+ait的和*/
e_1=lag(e);
dif=dif(e);
r=(dif+cou)/e_1;
run;

/*v6*/
data _null_;
set br_5_test1_&i;
if _n_=1 then do;
call symput("dw",d_w);
end;
run;

data br_5_test1_&i;
set br_5_test1_&i;
if _n_<=&dw then do;
r=-r;
end;
run;


/*v4新增*/

/*对于时间差仍然需要从新确认，已确保匹配的正确性 ps 解决*/
proc sql;
create table br_6_&i as
select * from br_5_test1_&i as a left join A.F_f as b on a.trd_exctn_dt-b.dat=0 or a.trd_exctn_dt-b.dat=1 or a.trd_exctn_dt-b.dat=2 or a.trd_exctn_dt-b.dat=3 or a.trd_exctn_dt-b.dat=4 or a.trd_exctn_dt-b.dat=5 or a.trd_exctn_dt-b.dat=6;
quit;

/*r-rf bond excess return
RF已乘以0.01*/
data br_6_&i;
set br_6_&i;
rf=RF*0.01;
r_rf=r-rf;
run;

data br_6_&i;
set br_6_&i;
r_rf=r_rf*100;
run;


/*v4新增*/
data b.b_r_&i;
set br_6_&i;
if entrd_vol_qt=0 then do;
r_rf='.';
end;
p_n=rptd_pr*entrd_vol_qt;
run;




/*delete useless*/

proc datasets lib=work  nolist;
		save br_6_&i / memtype=data;
		quit;


%mend bond_return;

%macro interests(ds,j);

data _null_;
set &ds;
call symput("fre",interest_frequency);
run;


/*fre 为利息支付的频率 其中m为m周需要支付一次利息*/
%if &fre=0 %then %do;
%let m=0;
%end;

%if &fre=1 %then %do;
%let m=%eval(52/&fre);
%end;


%if &fre=2 %then %do;
%let m=%eval(52/&fre);
%end;


%if &fre=4 %then %do;
%let m=%eval(52/&fre);
%end;

%if &fre=12 %then %do;
%let m=4;
%end;

%if &fre=14 %then %do;
%let m=4;
%end;

/*m将被用于 每m周交割一次利息，此时coupon为总coupon/fre，AIT为0.
  per用于观测利息交割，mod(d_2,m)计算了距离上一个交割周m相差几天，余数可以很好的反应这个问题
  再除以m则可以用于计算AIT*/
data br_5_test1_&j;
set br_5_test1_&j;
if &m=0 then per=0;
else per=(mod(d_w,&m))/&m;

/*体现真实的coupon*/
if &fre=0 then coup=coupon;
else coup=coupon/&fre;

/*AIT*/
if per=0 then A=0;
else A=round(coup*per,0.001);
run;


%mend  interests;

%macro def_rank(t);


%let q=0;
%let M_week=0;

data A.def2;
run;

%do i=1 %to &t;

proc sql;
create table max_&i as
select id
from B.b_r_&i
having id=max(id);
quit;

data _null_;
set max_&i;
call symput('q',id);
run;

proc datasets lib=work  nolist;
delete max_&i / memtype=data;
quit;

%if &q>&M_week %then %do;
%let M_week=%eval(&q.+0);
%end;

dm log 'clear;' continue;
%end;

%put &M_week;


%do j=1 %to &M_week;
data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf id p_n);
set %do i=1 %to 5000;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;

data rank_&j (keep=cusip_id rptd_pr entrd_vol_qt r_rf id p_n);
set rank_&j %do i=5001 %to 11263;B.b_r_&i. %end;;
where id=&j;
run;

dm log 'clear;' continue;




data rank_&j;
set rank_&j;
if r_rf='.' then delete;
run;

proc sql noprint;
create table rank_total_&j as
select r_rf,p_n,sum(p_n) as total
from rank_&j;
quit;

data rank_total_&j;
set rank_total_&j;
weight_r=(r_rf*p_n)/total;
run;

proc sql noprint;
create table rank_vw_&j as
select sum(weight_r) as def
from rank_total_&j
quit;

data A.def2;
set A.def2 rank_vw_&j;
run;

PROC DATASETS LIB = work  KILL nolist;
RUN;
quit;


dm log 'clear;' continue;

%end;


%mend def_rank;

%split(A.name_ig_1,A.name_ig_2,A.Trace_enhanced_clean_cut);
