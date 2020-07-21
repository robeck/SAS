# SAS
* SAS for bonds analysis

V8：
1.代码进一步精简，并且全部备注好说明，请使用V8文件夹中的程序。

V7：
1. 代码已全部修改，请使用已 ‘完成测试版本’ 文件中的程序进行操作。
2. 请注意新版本中需要用到两个因子，分别在文件夹data2中，分别存于def2和term数据集中。

具体改进：
a. 参数计算中加入了5因子。
b. 参数计算中加入了single-sorted with control参数。rsj->rsk和rsk->rsj。
c. 排序计算进行了优化，避免了过多无用文件的使用。



# SAS code explanation and description

# 1）. SAS程序运行说明：

第一步：计算29608个债券的bond excess return以及rsj，rkt等参数。
* 该步骤使用的程序有（return_calculate_V7_5factors_control_5999，return_calculate_V7_5factors_control_11998,...）共五个。

* 1.首先将data.rar和data2.rar文件解压到同一文件夹中，并将该文件夹绑定为SAS程序中逻辑库A。

* 2.在逻辑库中新建逻辑库B同时关联到空白文件夹（V7_29608） ps：用于储存全部计算结果此处文件夹命名为V7_29608。

* 3.（在逻辑库中新建逻辑库C同时关联到新空白文件夹（ds_29608)。ps：该步骤只需要执行一次，即可在C逻辑库中存储29608个粗处理数据集，目的为便捷以后的运算,此处文件夹命名为ds_29608。）

* 4.确认逻辑库A中必须包含有数据集 F_f, Name_info_new, Trace_enhanced_bond_cut, def2，term，name_1, name_2, name_3, name_4, name_5。

* 5.打开程序return_calculate_V7_5factors_control_5999.sas, 直接运行即可。

* 6.如果需要同时一次性计算全部数据，此时需要额外同时打开四个SAS程序，执行当前步骤中操作1->4, (此处许明确，每一个新sas程序中新建的逻辑库必须全部一致，且关联到文件夹V7_29608和ds_29608。)随后在四个程序中分别运行 ...._v7_11998, ....V7_17997, ....V7_23996,  ...._v7_29608。


第二步：对29608个债券，共计783周的数据进行排序计算。
* 该步骤使用的程序有(rank_v7_2_252_onefile_reserve.sas, rank_v7_252_502_onefile_reserve.sas, rank_v7_502_783_onefile_reserve.sas)共三个。

* 1.首先我们需要重新打开一个SAS程序，新建逻辑库B->关联到与第一步相同的文件夹V7_29608。

* 2.新建逻辑库C->关联到新文件夹，ps：此处文件夹可命名为Rank。

* 3.打开程序rank_v7_2_252_onefile_reserve.sas，直接运行即可。

* 4.同时再打开两个sas程序，执行1->2,请确保逻辑库B，C关联的文件必须一致，为V7_29608和Rank。随后分别运行rank_v7_252_502_onefile_reserve.sas和rank_v7_502_783_onefile_reserve.sas

ps：所有关联路径最好不要出现中文。
ps：783周为多次测算结果，直接使用即可，如需验证我将提供额外测试代码。



# 2）. 其他程序说明：

combine_final_step_v7： 该程序可一次性统计完全部rank文件中的数据集，运行方法为：当第二步全部运行完成后，在任意sas程序中打开combine_final_step_v7.sas，直接运行即可。最终结果将在rank文件夹中，分别为数据集rsj，rovl，rsk，rkt。

newey_west:  该程序用于进行T检验


# 2）. SAS程序中各函数细节说明

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
        %do %while(%scan(%quote(&names),&i,',') ne %str());/*子串不为空是循环拆分数据集*/
                %let dname=%scan(%quote(&names),&i,',');
				
				
				data D_&i;
                set &ds1;
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
				
				/*将粗处理的数据存储到逻辑库C中，从而我们可以进一步简化后续处理*/
				data C.Ds_&i;
				set DS_&i;
				run;

				proc datasets lib=work  nolist;
				delete D_&i / memtype=data;
				quit;
				
				/*
				loop（&i）函数对每个拆分出来的债券数据进行计算，包括bond_return和其他排序参数
				*/
				%loop(&i);
				
	
                %let i=%eval(&i.+1);
				
				 dm log 'clear;' continue; 
		

        %end;
		


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

	%mend loop;





/**********

对每一个债券数据集ds进行计算，其输出数据集包含bond excess return和 Q（5 factors ff regression）

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
	from br_5_&i as a left join br_2_test2_&i as b on a.trd_exctn_dt-b.trade_date=0 or a.trd_exctn_dt-b.trade_date=1 or a.trd_exctn_dt-b.trade_date=2 or a.trd_exctn_dt-b.trade_date=3 or a.trd_exctn_dt-b.trade_date=4 or a.trd_exctn_dt-b.trade_date=5 or a.trd_exctn_dt-b.trade_date=6;
	quit;


	data br_5_test1_&i;
	set br_5_test1_&i;
	if rptd_pr=0 then do;
	rptd_pr=pr;
	end;

	drop p q trade_date pr;
	run;


	/*
	 dm 计算了从第一笔交易开始，到首次付息日所差的天数
	 dw 计算了这些天数所在的周
	 abs取绝对值，用于计算准确交割周
	*/
	data br_5_test1_&i;
	set br_5_test1_&i nobs=nobs;
	d_m=first_interest_date-trd_exctn_dt;
	d_w=ceil(round(d_m/7,0.001));
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



	/*匹配3因子参数*/
	proc sql;
	create table br_6_&i as
	select * from br_5_test1_&i as a left join A.F_f as b on a.trd_exctn_dt-b.dat=0 or a.trd_exctn_dt-b.dat=1 or a.trd_exctn_dt-b.dat=2 or a.trd_exctn_dt-b.dat=3 or a.trd_exctn_dt-b.dat=4 or a.trd_exctn_dt-b.dat=5 or a.trd_exctn_dt-b.dat=6;
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



