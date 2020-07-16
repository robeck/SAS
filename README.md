# SAS
* SAS for bonds analysis

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

1）test_join_name_2020test_v6程序说明

/*split主程序用于将ds中包含的债券信息与ds1中数据匹配对应，生成完整的债券数据集，同时匹配name_info_new数据集，最终完成数据集DS_i
  共29608个债券*/
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

				proc datasets lib=work  nolist;
				delete D_&i / memtype=data;
				quit;

				%loop(&i);
				
	
                %let i=%eval(&i.+1);
				
				 dm log 'clear;' continue; 
		

        %end;
%mend split;	


