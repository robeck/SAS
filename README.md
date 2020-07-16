# SAS
* SAS for bonds analysis

请使用已‘完成测试版本’文件中的程序进行操作
请注意新版本中需要用到两个因子，def和term，分别是数据集。。。。，

# SAS code explanation and description

# 1. SAS程序运行说明：

第一步：计算29608个债券的bond excess return以及rsj，rkt等参数。
* 该步骤使用的程序有（return_calculate_V7_5factors_control_5999，return_calculate_V7_5factors_control_11998,...）共五个。

* 首先将data.rar和data2.rar文件解压到同一文件夹中，并将该文件夹绑定为SAS程序中逻辑库A Example code：libname A 'F/:.....';run;

* 在逻辑库中新建逻辑库B同时关联到空白文件夹。

* 确认逻辑库A中必须包含有数据集 F_f, Name_info_new, Trace_enhanced_bond_cut, name_1, name_2, name_3, name_4, name_5。

* 打开程序return_calculate_V7_5factors_control_5999, 直接运行即可。

* 如果需要同时一次性计算全部数据，此时需要额外同时打开四个SAS程序，执行当前步骤中操作1->3。随后在四个程序中分别运行 ...._v6_11998, ....V6_17997, ....V6_23996,  ...._v6_29608。


第二步：对29608个债券，共计783周的数据进行排序计算。

* 首先需要重新打开一个SAS程序，建立逻辑库B关联和第一步相同的文件夹（此步操作中逻辑库B应该包含有第一步计算完成的29608个数据集）。

* 同时新建C，D，E，F四个逻辑库，分别关联到不同文件夹（一定要是不同文件夹） ps：如有必要请备注C所关联的文件夹为包含参数RSJ，D文件夹包含参数RSK，E包含RKT， F包含ROVL。

* 打开程序Rank_v6_2_252, 直接运行即可。

* 与此同时打开额外两个SAS程序，执行当前步骤操作1->2。ps: 此时一定要注意，每次新建的C D E F逻辑库必须关联到不同文件夹，绝对不能共同使用，即使同为C逻辑库 其关联文件夹也必须不一样。

* 随即即可分别运行Rank_v6_252_502，Rank_v6_502_783。

ps：所有关联路径最好不要出现中文。
ps：783周为多次测算结果，直接使用即可，如需验证我将提供额外测试代码。



2. 其他程序说明：

Combine.sas： 该程序使用是在以上步骤都计算完成后。

* 新建SAS程序，将第二步中关联的3个不同文件夹的C逻辑库，在新程序中分别重新关联并命名为C，D，E逻辑库。

* 同时新建F逻辑库关联新文件夹，用以保存完整的数据集。

* 运行该程序，可在F逻辑库在获得按照不同参数排序，分组后的五个最终数据集。

final_step:  该程序用于计算加权平均，算数平均。

newey_west:  该程序用于进行T检验


# 2. SAS程序中各函数细节说明

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


