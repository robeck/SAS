

data w_5(keep=war5 id);
set Rsj.w_1;
war5=aver_q;
if war5='.' then delete;
id=_n_;
run;

data w_1(keep=war1 id);
set Rsj.w_5;
war1=aver_q;
if war1='.' then delete;
id=_n_;
run;

data w_5_1(drop=id);
merge w_5 w_1;
by id;
if war5='.' then war5=0;
if war1='.' then war1=0;
run;

data w_5_1_dif(keep=high_low);
set w_5_1;
high_low=war5-war1;
run;

proc means data=w_5_1_dif mean;
   var high_low;
run;

proc model data=w_5_1_dif;
endo high_low;
instruments / intonly;
   parms b0;
   high_low=b0;
   fit high_low / gmm kernel=(bart,5,0) vardef=n;
run;
quit;



