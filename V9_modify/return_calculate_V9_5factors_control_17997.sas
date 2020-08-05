/*****************************************
  1.匹配term因子，已完成。（完成）


  2.计算def因子，随后匹配
	a.首先需要基于目前的代码计算def，第一步需要计算10000多个符合要求的债券return数据
	b.第二步需要对计算的债券按周排序，并且计算每周中的value_weight_return
		I.计算思路：对def进行单独的计算，排序操作
		II.综合在全部计算中一起进行
  最好方案：单独计算def，便于检查问题。
  计算中。（完成）

  3.五因子计算与测试（未完成）

  4.single-sorted portfolios with control  rsj-》rsk已经完成（完成）
  5.额外需要考虑何时使用rsk->rsj的残差估计 （完成）

ps：注意def因子在A.def2中，目前还需要进行计算才可以完全匹配上 （完成）
ps: 该版本用于非control的五因子依然适用


V7版本已完成所有可用参数的计算，需要注意C逻辑库储存的粗处理数据后续可以直接使用，因此V8版本是存在的，后续还会更新
**********************************************/




/***********************************
split(ds,ds1)
用于拆分，匹配总共29608个债券的全部数据

&nemas:包含单一债券名称的宏数据
ds_&i:拆分出来包含必要信息的债券数据集，用于之后的计算


************************************/
%macro split(ds,ds1);
      
        proc sql noprint;
        select distinct cusip_id into: names separated by ','/*所有类别放入宏names，逗号分隔*/
        from &ds;
        quit;
        %let i=1;
        %let o=11999;
        %do %while(%scan(%quote(&names),&i,',') ne %str());/*子串不为空是循环拆分数据集*/
                %let dname=%scan(%quote(&names),&i,',');
				
				
				data D_&o;
                set &ds1;
                where cusip_id = "&dname";
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
                %let i=%eval(&i.+1);
				
				
		 dm log 'clear;' continue; 

        %end;
		
		
		



		
%mend split;


/*******************
对每个拆分出来的债券数据进行计算，包括bond_return和其他排序参数

 %bond_return(Ds_&i,&i): This function is used to calculate bond_return
 %calculate(&i): This function is used to calculate rsj, rsk, rkt, rovl and others
 %merge(&i): this function merges all datasets within one single bond which contains different paramaters  

***************************/
%macro loop(i);

 %bond_return(Ds_&i,&i);
 %calculate(&i);
 %merge(&i);


 dm log 'clear;' continue;/*clear logs*/

%mend loop;


/**********
对每一个债券数据集ds进行计算，其输出数据集包含bond excess return和 Q（5 factors ff regression）。
ds: its a dataset which will be calculated.
i: it represents bond i.

bond_return()函数实现了
1.对短期缺失数据的均值补充，以及长期缺失数据的前值覆盖。
2.根据数据补充的结果，再进行对bond excess return值和回归因子Q的计算。
***********/
%macro bond_return(ds,i);

/****

step1


****/
/*此处代码块用于计算每一天的平均价格price 和总成交量quantity，便于后面实现数据填补*/
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

/****

step2：对长期缺失数据做补充，数据集br_2_test2_&i中保存长期缺失数据


****/
/*
对bond return做数据真实性补充，即没有交易的日期中价格将由前一笔交易价格所替代。
首先对数据做每天连续填补，没有交易的日期所需要填补的价格为前一比交易的价格。
*/
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
			     _price=_price;
		 		 _quantity=0;
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

/*保证每一个债券交易起始日期为周三*/
data br_2_test1_&i;
set br_2_test1_&i;
wd=WEEKDAY(trd_exctn_dt);
if _n_=1 then do;
if wd>3 then call symput('e',7-(wd-3));
if wd<3 then call symput('e',3-wd);
if wd=3 then call symput('e',0);
end;
run;

/*删除周末*/
data br_2_test1_&i;
set br_2_test1_&i;
%put &e;
if _N_<=&e then delete;
if wd=6 or wd=7 then delete;
run;


/*最终生成的br_2_test2_&i数据集包含了所需要填补的交易价格，但并无交易量存在*/
data br_2_test1_&i;
set br_2_test1_&i;
rename trd_exctn_dt=trade_date;
rename rptd_pr=price;
run;

data br_2_test1_&i(keep=cusip_id trade_date price);
set br_2_test1_&i;
run;

/*7.17 modify,保证每周最后一笔交易价格与前值相同*/
data br_2_test1_&i;
set br_2_test1_&i;
t=mod(_n_,5);
if t=0 then t=5;

retain pr5;
if t=5 then do;
pr5=price;
end;
if t=5 then output;
run;


data br_2_test2_&i;
set br_2_test1_&i;
pr=pr5;
drop pr5 price t;
run;

data br_2_test2_&i;
set br_2_test2_&i;
nid=_n_;
run;



/****

step3：统计每周中真实存在的交易天数，并作统计

*****/
/*这里增加了参数cound_day_a，可以计算缺失数据的相隔天数，从而确认周中真实交易天数*/
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


/*该处的三步为了统计交易中缺失的天，即没有交易的日期被标记为1*/
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

/*取五天为一个周期，并且计算这一周中有几天缺失数据统计为count_5，如count_5=4则表示该周中4天无交易*/
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

/****

step4：当存在一周中有多比交易价格时，该周中最后一笔交易价格将由本周交易价格的平均值替代

****/
/*重新计算count_5为有交易的天数，即在数据集br_4_&i中count_5=4表示一周中有4天有交易，然后统计每周的平均交易价格做为该周最后一笔交易，记为rptd_pr_5*/
data br_4_&i;
set br_3_&i;
count_5=5-count_5;

if rptd_pr_5^=0 then do;
rptd_pr_5=rptd_pr_5/count_5;
entrd_vol_qt_5=entrd_vol_qt_5/count_5;
end;
run;

/*该处的四步为了填补缺失仅一周的数据，即连续三周中仅有中间一周缺失的情况，这种情况做除数据填补，方法为前后两周数据的平均*/
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



/****

step5：短期周缺失填补

****/
/*数据集br_5_&i 组合多组数据，实现对短期缺失周数据的补充*/
data br_5_&i;
merge br_4_&i br_4_3_&i;
by id;
run;
 
/*计算短期要填补的周的数据*/
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



/****

step6：完成全部数据填补，并开始计算bond_excess_return和5因子回归参数Q

****/
/*完成最终的数据填补，即短期缺失数据以两周均值填补，长期缺失*/
proc sql noprint;
create table br_5_test1_&i as
select *
from br_5_&i as a left join br_2_test2_&i as b on a.id=b.nid;
quit;


data br_5_test1_&i;
set br_5_test1_&i;
if rptd_pr=0 then do;
rptd_pr=pr;
end;

drop p q trade_date pr nid;
run;


/*7.21调整*/
/*
 dm 计算了从第一笔交易开始，到首次付息日所差的天数
 dw 计算了这些天数所在的周
 abs取绝对值，用于计算准确交割周
*/
data br_5_test1_&i;
set br_5_test1_&i nobs=nobs;
d_m=first_interest_date-trd_exctn_dt;
d_w=ceil(round(d_m/7,0.001));
if d_w>0 then do;
d_w=52-d_w;
end;
d_w=abs(d_w);
run;




/*用来计算AIT，即债券应记利息 */
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


/**对第一次交割前的情况，我们重算了dw值
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

**/


/*7.21调整*/
/*匹配3因子参数*/
proc sql;
create table br_6_&i as
select * from br_5_test1_&i as a left join A.F_f as b on a.trd_exctn_dt-b.dat=0 or a.trd_exctn_dt-b.dat=1 or a.trd_exctn_dt-b.dat=2 or a.trd_exctn_dt-b.dat=3 or a.trd_exctn_dt-b.dat=4;
quit;

/*匹配term因子*/
proc sql;
create table br_7_&i as
select * 
from br_6_&i as a left join A.lgbt_ombill as b on a.dat-b.ddate=0 or a.dat-b.ddate=1 or a.dat-b.ddate=2 or a.dat-b.ddate=3;
quit;

/*匹配def因子*/
proc sql noprint;
create table br_8_&i as
select *
from br_7_&i as a left join A.def2 as b on a.id=b.id;
quit;


/*计算bond excess return
r-rf = bond excess return
RF已乘以0.01*/
data br_8_&i;
set br_8_&i;
rf=RF*0.01;
r_rf=r-rf;
run;

data br_8_&i;
set br_8_&i;
r_rf=r_rf*100;
run;




/*linear reg for f-f3 用于计算艾尔法Q值*/
/*采用全新的ff5因子计算Q值*/
%reg(br_8_&i);

/*此处的代码用以清洁部分脏数据-即数据集为空的情况,尤其是存在Q值为字符串而非数值，可能导致排序中出错*/
data br_8_&i;
set br_8_&i;
if entrd_vol_qt=0 then do;
r_rf='.';
Q='.';
end;
run;

data br_8_&i;
	set br_8_&i;
	Q_new=input(Q,12.);
	r_rf_new=input(r_rf,12.);
	rename Q_new=Q;
	rename r_rf_new=r_rf;
	drop Q r_rf;
run;

data _null_;
if 0 then set br_8_&i  nobs=count;
call symput('obs', count);
run;


%put &obs;

%if &obs=0 %then %do;
data br_8_&i;
set br_8_&i;
drop Q;
run;
%end;





%mend bond_return;


/********************
reg（）函数实现了
1.对数据集ds做线性回归，截距Intercept保留，即为我们需要的Q值

ds:用于计算回归的数据集
*********************/
%macro reg(ds);

/*五因子回归，这里需要注意变量名称：def，term是否正确*/
proc reg data=&ds outest=est noprint;
model r_rf=Mkt_RF SMB HML term def;
quit;

data _null_;
set est;
call symput("c",Intercept);
run;

data &ds;
set &ds;
Q=&c;
run;

%mend reg;

/****************
interests（）函数实现了
1.根据数据集ds的不同交割频率，我们计算对应的应计利息AIT

ds:用于计算利息的数据集
j:第j个债券
******************/
%macro interests(ds,j);

data _null_;
set &ds;
call symput("fre",interest_frequency);
run;


/*fre为利息支付的频率 其中m：每m周需要支付一次利息*/
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



/***************************
calculate（）函数实现了
1.对缺失数据的前值覆盖，即缺失周或者天的债券价格按照前值进行填补，例如初始价格$90,接下来有10天无交易，其价格将按照前值即初始价格$90来补充。
2.根据每天交易价格的波动数据来计算各个参数包括，rsj, rkt, rsk, rovl以及residual。

n:第n个债券
**************************/
%macro calculate(n);

/****

step1


****/
/*此处代码块用于计算每一天的平均价格price 和总成交量quantity，便于后面实现数据填补*/
proc sql;
create table Ds_1_&n as
select *, sum(entrd_vol_qt) as total_quan, sum(total) as day_total
from Ds_&n
group by trd_exctn_dt;
quit;

data Ds_2_&n;
set Ds_1_&n;
pri=round((day_total/total_quan),0.001);

data Ds_2_&n;
set Ds_2_&n(drop=rptd_pr entrd_vol_qt day_total total rename=(total_quan=entrd_vol_qt) rename=(pri=rptd_pr));
label rptd_pr='Price';
label entrd_vol_qt='quantity';
run;

proc sql;
create table Ds_3_&n as
select  * from Ds_2_&n
where id in (select max(id) from Ds_2_&n group by trd_exctn_dt);
quit;


/****

step2：数据填补


****/
/*其方法为：当出现某日无债券交易的情况，我们并不直接将无交易的日期中债券价格设置为0，而是将该日债券价格设置为前值价格，目的为保证无交易=无价格波动*/
data Ds_4_&n(rename=(_date=trd_exctn_dt) rename=(_price=rptd_pr) rename=(_quantity=entrd_vol_qt));
   set Ds_3_&n(drop=id);
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
			     _price=_price;
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

/*以每周三为起始，一周共保留五天交易日*/
data Ds_4_&n;
set Ds_4_&n;
wd=WEEKDAY(trd_exctn_dt);
if _n_=1 then do;
if wd>3 then call symput('e',7-(wd-3));
if wd<3 then call symput('e',3-wd);
if wd=3 then call symput('e',0);
end;
run;

/*不计算周末，删除周末*/
data Ds_4_&n;
set Ds_4_&n;
%put &e;
if _N_<=&e then delete;
if wd=6 or wd=7 then delete;
run;


/****

step3：参数计算


****/
/*
计算每日每笔交易差值

1.dif1代表的每日交易差，即rt
2.dif2=rt的平方
3.dif3=rt的立方
4.dif4=rt的四次方
*/
data ds_5_&n;
set Ds_4_&n;
dif1=dif(rptd_pr);
dif2=dif1**2;
dif3=dif1**3;
dif4=dif1**4;
num=_n_-1;
w=int(num/5)+1;
drop num;
run;


/*每五天为一周，计算周化的rt的和，平方和，三次方和以及四次方和*/
data ds_6_&n(drop=dif1 dif2 dif3 dif4);
set ds_5_&n;
t=mod(_n_,5);
	if t=0 then t=5;

	retain dif1_5 dif2_5 dif3_5 dif4_5;
		if t=1 then do;
		dif1_5=dif1;
		dif2_5=dif2;
		dif3_5=dif3;
		dif4_5=dif4;
	end;
	else do;
		dif1_5=dif1_5+dif1;
		dif2_5=dif2_5+dif2;
		dif3_5=dif3_5+dif3;
		dif4_5=dif4_5+dif4;
	end;
	if t=5 then output;
	run;

/*这里选取rt>0和rt<0的不同部分，区分交易波动的符号特性是为了计算rsj参数*/
data ds_7p_&n ds_7n_&n;
set ds_5_&n;
if dif1>=0 then output ds_7p_&n;
if dif1<0 then output ds_7n_&n;
run;

/*
  rt>0部分的平方和pos
  rt<0部分的平方和neg
*/
proc sql;
create table ds_7p_1_&n as
select cusip_id,trd_exctn_dt,sum(dif2) as rvt_pos,w
from ds_7p_&n
group by w
order by trd_exctn_dt;

create table ds_7n_1_&n as
select cusip_id,trd_exctn_dt,sum(dif2) as rvt_neg,w
from ds_7n_&n
group by w
order by trd_exctn_dt;

quit;

data ds_7p_1_&n;
set ds_7p_1_&n;
by w;
if last.w then output;
run;

data ds_7n_1_&n;
set ds_7n_1_&n;
by w;
if last.w then output;
run;

data ds_8_&n;
	merge ds_7p_1_&n(drop=trd_exctn_dt)
	      ds_7n_1_&n(drop=trd_exctn_dt);
	by w;

	if rvt_pos='.' then rvt_pos=0;
	if rvt_neg='.' then rvt_neg=0;
	run;

/*7.21修改*/
/*计算参数rsj,rsk,rkt,rovl*/
data ds_9_&n(keep=cusip_id rsj rsk rkt rovl id);
	merge ds_6_&n(keep=w dif1_5 dif2_5 dif3_5 dif4_5)
		  ds_8_&n;
	by w;

	if dif1_5='.' then delete;
	sj_t= rvt_pos-rvt_neg;
	rsj=sj_t/dif2_5;
	/*7.21修改*/
	rsk=((sqrt(5))*dif3_5)/(sqrt(dif2_5**3));
	rkt=(5*dif4_5)/dif2_5**2;
	/*7.21修改*/
	rovl=(252/5)*sqrt(dif2_5);
	id=_n_;
run;

/*
rsj->rsk 的残差，保存在tt_&n中，标记为resid_rsj_rsk
rsk->rsj 的残差，保存在t_&n中，标记为resid_rsk_rsj
*/
proc reg data=ds_9_&n noprint;
model rsj=rsk;
output out=tt_&n stdr=stdr r=resid_rsj_rsk STUDENT=stud;
  run;
quit;

proc reg data=ds_9_&n noprint;
model rsk=rsj;
output out=t_&n stdr=stdr r=resid_rsk_rsj STUDENT=stud;
  run;
quit;

data tt_&n;
set tt_&n;
keep id resid_rsj_rsk;
run;

data t_&n;
set t_&n;
keep id resid_rsk_rsj;
run;

data ds_9_&n;
merge ds_9_&n tt_&n t_&n;
by id;
run;



/*若有空值参数设置为0*/
%nullo(ds_9_&n);


proc datasets lib=work  nolist;
save br_8_&n ds_9_&n / memtype=data;
quit;



%mend calculate;


/**************************
nullo()函数实现了
1.确保ds数据集中无空值，所有空值由0替代，便于后期排序操作
**************************/
%macro nullo(ds);

	data &ds;
	set &ds;
	array numtmp _numeric_;
	do over numtmp;
	numtmp=coalesce(numtmp,0);
	end;
	run;


%mend nullo;


/**************************
merge()函数实现了
1.组合数据集br_8_i和ds_9_i,即把周化的包含bond excess return数据集和包含排序参数的数据集组合在一起，便于我们在后期排序中，根据参数大小的不同来统计return和Q值

m:第m个债券
**************************/
%macro merge(m);

proc sql;
create table b_r_&m as
select * from br_8_&m as a left join ds_9_&m as b on a.id=b.id;
quit;

/*存入新逻辑库B*/
data B.b_r_&m;
set b_r_&m;
p_n=rptd_pr*entrd_vol_qt;
if dat=' ' then delete;
run;

proc datasets lib=work  nolist;
delete br_8_&m b_r_&m ds_9_&m / memtype=data;
quit;


%mend merge;

%split(A.name_3,A.Trace_enhanced_clean_cut);
