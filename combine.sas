%macro combine(C,D,E,F);

%do i=1 %to 5;

data w_&i;
set &&C .w_&i &&D .w_&i &&E .w_&i ;
run;

data &&F .w_&i;
set w_&i;
if w_aver_q='.' then delete;
run;


%end;

%mend combine;


%combine(C,D,E,F);
