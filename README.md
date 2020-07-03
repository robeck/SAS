# SAS
SAS for bonds analysis
SAS code explanation and description

SAS程序运行说明：

第一步：计算29608个债券的bond excess return以及rsj，rkt等参数。

·首先将data.rar和data2.rar文件解压到同一文件夹中，并将该文件夹绑定为SAS程序中逻辑库A
 Example code：libname A 'F/:.....';run;
·在逻辑库中新建逻辑库B同时关联到空白文件夹。
·确认逻辑库A中必须包含有数据集 F_f, Name_info_new, Trace_enhanced_bond_cut, name_1, name_2, name_3, name_4, name_5。
·打开程序test_join_name_new_2020test_v6_5999, 直接运行即可。
·如果需要同时一次性计算全部数据，此时需要额外同时打开四个SAS程序，执行当前步骤中操作1->3。随后在四个程序中分别运行 ...._v6_11998, ....V6_17997, ....V6_23996,  ...._v6_29608。

第二步：对29608个债券，共计783周的数据进行排序计算。

·首先需要重新打开一个SAS程序，建立逻辑库B关联和第一步相同的文件夹（此步操作中逻辑库B应该包含有第一步计算完成的29608个数据集）。
·同时新建C，D，E，F四个逻辑库，分别关联到不同文件夹（一定要是不同文件夹），如有必要请备注C所关联的文件夹为包含参数RSJ，D文件夹包含参数RSK，E包含RKT， F包含ROVL。
·打开程序Rank_v6_2_252, 直接运行即可。
·与此同时打开额外两个SAS程序，执行当前步骤操作1->2。ps:此时一定要注意，每次新建的C D E F逻辑库必须关联到不同文件夹，绝对不能共同使用，即使同为C逻辑库 其关联文件夹也必须不一样。
·随即即可分别运行Rank_v6_252_502，Rank_v6_502_783。

ps：所有关联路径最好不要出现中文。
ps：783周为多次测算结果，直接使用即可，如需验证我将提供额外测试代码。



其他程序说明：

·Combine.sas： 该程序使用是在以上步骤都计算完成后。
