# SAS
* SAS for bonds analysis

v9_modify：

1.此代码版本为全部运行版本，即single_sorted,single_sorted_controlling,double_sorted,cross_section_regression等全部功能一次运行实现。

2.代码请使用v9_modify文件夹中的程序，必要数据集使用data2文件夹。

3.代码的复杂性，和运行的复杂性是由于时间因素所导致的，分块运行代码能大大提升代码运行效率。鉴于sas无法实现真正的多线程操作，多代码块，多逻辑库关联是不得不做的

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

第一步：计算29608个债券的bond excess return以及rsj，rkt，rkt,rovl,rsj_rsk_resdial,rsk_rsj_resdiual等参数。
* 该步骤使用的程序有（return_calculate_V9_5factors_control_5999，return_calculate_V9_5factors_control_11998,...）共五个。

* 1.首先将data.rar和data2.rar文件解压到同一文件夹中，并将该文件夹绑定为SAS程序中逻辑库A。

* 2.确认逻辑库A中必须包含有数据集 F_f, Name_info_new, Trace_enhanced_bond_cut, def2，term，name_1, name_2, name_3, name_4, name_5。

* 3.在逻辑库中新建逻辑库B同时关联到空白文件夹（例如空白文件夹命名：V9_29608） ps：用于储存全部计算结果此处文件夹命名为V9_29608。


* 4.打开程序return_calculate_V9_5factors_control_5999.sas, 直接运行即可。

* 5.如果需要同时一次性计算全部数据，此时需要额外同时打开四个SAS程序，执行当前步骤中操作1->3, (此处许明确，每一个新sas程序中新建的逻辑库必须全部一致，且关联到文件夹V9_29608。)随后在四个程序中分别运行 ...._v9_11998, ....V9_17997, ....V9_23996,  ...._v9_29608。


第二步：对29608个债券，共计783周的数据进行排序计算。
* 该步骤使用的程序有(rank_v10_252_double_single_crossesction.sas, rank_v10_502_double_single_crossesction.sas, rank_v10_783_double_single_crossesction.sas)共三个。

* 1.首先我们需要重新打开一个SAS程序，新建逻辑库B->关联到与第一步相同的文件夹:V9_29608。

* 2.新建逻辑库C->关联到新文件夹，ps：此处文件夹可命名为: double_Rank。此处存放double_sorted的数据。

* 3.新建逻辑库D->关联到新文件夹，ps：此处文件夹可命名为: single_Rank。此处存放single_sorted和single_sorted_controlling的数据。

* 4.新建逻辑库E->关联到新文件夹，ps：此处文件夹可命名为: cross_Rank。此处存放cross_section_regression的数据。

* 3.打开程序rank_v10_252_double_single_crossesction.sas，直接运行即可。

* 4.同时再打开两个sas程序，执行1->2,请确保逻辑库B,C,D,E关联的文件必须一致。随后分别运行rank_v10_502_double_single_crossesction.sas和rank_v10_783_double_single_crossesction.sas

ps：所有关联路径最好不要出现中文。
ps: 请备份好C，D，E逻辑库关联的文件，以防后期处理中导致数据集改变。
ps：783周为多次测算结果，直接使用即可，如需验证我将提供额外测试代码。




# 2）. 其他程序说明：

combine_final_All：

* 1.当前版本的combine_final_all一次性可以处理全部数据集，因此我们直接在全部rank运行完后，随机在一个程序中打开代码 combine_final_All.sas

* 2.直接运行combine_final_All,在C，D，E逻辑库中均会出现最终的数据集结果。


ps:关于t检验和其他说明，后续将补充


# 2）. SAS程序中各函数细节说明


# 首先说明代码：return_calculate_V7_5factors_control_5999.sas
	

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
	*/
	data ds_5_&n;
	set Ds_4_&n;
	dif1=dif(rptd_pr);
	if wd=3 then dif1=0;
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

	/*计算参数rsj,rsk,rkt,rovl*/
	data ds_9_&n(keep=cusip_id rsj rsk rkt rovl id);
		merge ds_6_&n(keep=w dif1_5 dif2_5 dif3_5 dif4_5)
			  ds_8_&n;
		by w;

		if dif1_5='.' then delete;
		sj_t= rvt_pos-rvt_neg;
		rsj=sj_t/dif2_5;
		rsk=((sqrt(5))*dif3_5)/(sqrt(sqrt(dif2_5)**3));
		rkt=(5*dif4_5)/dif2_5**2;
		rovl=sqrt(dif2_5);
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








# 其次说明代码 rank_v7_252_502_onefile_reserve.sas

	/******************************************
	V7 更新：
	1.将按照各参数排序的qunity放入同一文件夹中，便于编程使用
	2.保留residual参数，便于日后使用
	******************************************************

	V8 更新：
	1.进一步优化代码结构，删除了不必要的部分。
	2.由于没有新的函数出现，命名上不做更改
	****************************************************

	程序简要说明：
	1. 逻辑结构说明：
		a.根据统计出来的周化数据中最长周数783周为依据，我们首先按照周数来提取债券数据，例如；第一周，我们提取29608个债券中有第一周数据的债券，组成新的数据集rank_j。以此类推，直到783	周，我们就可以获得至多783个数据集。
		b.对每一个rank_j我们执行拆分（split）操作，即我们需要对每一周的数据按照参数排序后分成5组qunities。
		c.对每组qunity中的数据统计bond excess return和Q值，计算加权平均和算数平均。

	2. 主要函数块说明：
	rank（）：提取每个债券在特定周中的数据，最终组合成数据集rank_j,随后执行split操作。
	split（）：对rank_j按照特定参数排序，同时计算每一周中有多少债券，以确认划分的5组中需要包含多少数据。
		sep（）：用以完成split操作，划分qunities并且生成新数据集。
		delet（）：删除空白数据集，用来节约内存空间。
	cal（）：用来对分组好的数据进行计算，统计结果到特点数据集。



	*******************************************/





/***********************************

函数rank（）为主函数，主要实现了：

1.抽取，组合每一周全部债券的数据，组成周数据集。

2.调用split（）函数，实现对周数据的分组操作。

3.调用cal（）函数，实现对加权平均和算数平均的计算

t:数据集个数

************************************/

	%macro rank(t);


	%let M_week=252;



	/*这里多个数据集合并的时候会造成内存崩溃，控制一次合并数据集的数量，保证运行顺利*/
	%do j=2 %to &M_week;/*循环共M_week周*/
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


	%do l=2 %to &h;

	/*迭代的部分，每次调用不同参数v都会有不同排序结果*/
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





/*****************************

删除观测为0的数据集

******************************/

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

	%macro cal(r,s,v);

	%let t=1;
	%let y=2;
	%let g=3;
	%let h=4;
	%let b=5;
	/*这里创建的数据集为分组5组时*/
	data C.Q_252_&v&t C.Q_252_&v&y C.Q_252_&v&g C.Q_252_&v&h C.Q_252_&v&b;
	run;


	%do i=2 %to &s;


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
	w_aver_q=QW/nobs;
	w_aver_r=RRFW/nobs;
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
	data C.Q_252_&v&j;
	set C.Q_252_&v&j &w&j;
	run;
	%end;

	%if &j=2 %then %do;
	data C.Q_252_&v&j;
	set C.Q_252_&v&j &w&j;
	run;
	%end;

	%if &j=3 %then %do;
	data C.Q_252_&v&j;
	set C.Q_252_&v&j &w&j;
	run;
	%end;

	%if &j=4 %then %do;
	data C.Q_252_&v&j;
	set C.Q_252_&v&j &w&j;
	run;
	%end;

	%if &j=5 %then %do;
	data C.Q_252_&v&j;
	set C.Q_252_&v&j &w&j;
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

	%mend cal;









