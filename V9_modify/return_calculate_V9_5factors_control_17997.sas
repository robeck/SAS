/*****************************************
  1.ƥ��term���ӣ�����ɡ�����ɣ�


  2.����def���ӣ����ƥ��
	a.������Ҫ����Ŀǰ�Ĵ������def����һ����Ҫ����10000�������Ҫ���ծȯreturn����
	b.�ڶ�����Ҫ�Լ����ծȯ�������򣬲��Ҽ���ÿ���е�value_weight_return
		I.����˼·����def���е����ļ��㣬�������
		II.�ۺ���ȫ��������һ�����
  ��÷�������������def�����ڼ�����⡣
  �����С�����ɣ�

  3.�����Ӽ�������ԣ�δ��ɣ�

  4.single-sorted portfolios with control  rsj-��rsk�Ѿ���ɣ���ɣ�
  5.������Ҫ���Ǻ�ʱʹ��rsk->rsj�Ĳв���� ����ɣ�

ps��ע��def������A.def2�У�Ŀǰ����Ҫ���м���ſ�����ȫƥ���� ����ɣ�
ps: �ð汾���ڷ�control����������Ȼ����


V7�汾��������п��ò����ļ��㣬��Ҫע��C�߼��ⴢ��Ĵִ������ݺ�������ֱ��ʹ�ã����V8�汾�Ǵ��ڵģ������������
**********************************************/




/***********************************
split(ds,ds1)
���ڲ�֣�ƥ���ܹ�29608��ծȯ��ȫ������

&nemas:������һծȯ���Ƶĺ�����
ds_&i:��ֳ���������Ҫ��Ϣ��ծȯ���ݼ�������֮��ļ���


************************************/
%macro split(ds,ds1);
      
        proc sql noprint;
        select distinct cusip_id into: names separated by ','/*�����������names�����ŷָ�*/
        from &ds;
        quit;
        %let i=1;
        %let o=11999;
        %do %while(%scan(%quote(&names),&i,',') ne %str());/*�Ӵ���Ϊ����ѭ��������ݼ�*/
                %let dname=%scan(%quote(&names),&i,',');
				
				
				data D_&o;
                set &ds1;
                where cusip_id = "&dname";
                run;

				proc sql noprint;
				create table Ds_&o as
				select * from D_&o as a left join A.name_info_new as b on a.cusip_id=b.cusip_id;
				quit;

				/*ԭʼ�����Ѿ�����Ҫ������*/
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
��ÿ����ֳ�����ծȯ���ݽ��м��㣬����bond_return�������������

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
��ÿһ��ծȯ���ݼ�ds���м��㣬��������ݼ�����bond excess return�� Q��5 factors ff regression����
ds: its a dataset which will be calculated.
i: it represents bond i.

bond_return()����ʵ����
1.�Զ���ȱʧ���ݵľ�ֵ���䣬�Լ�����ȱʧ���ݵ�ǰֵ���ǡ�
2.�������ݲ���Ľ�����ٽ��ж�bond excess returnֵ�ͻع�����Q�ļ��㡣
***********/
%macro bond_return(ds,i);

/****

step1


****/
/*�˴���������ڼ���ÿһ���ƽ���۸�price ���ܳɽ���quantity�����ں���ʵ�������*/
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

step2���Գ���ȱʧ���������䣬���ݼ�br_2_test2_&i�б��泤��ȱʧ����


****/
/*
��bond return��������ʵ�Բ��䣬��û�н��׵������м۸���ǰһ�ʽ��׼۸��������
���ȶ�������ÿ���������û�н��׵���������Ҫ��ļ۸�Ϊǰһ�Ƚ��׵ļ۸�
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

/*��֤ÿһ��ծȯ������ʼ����Ϊ����*/
data br_2_test1_&i;
set br_2_test1_&i;
wd=WEEKDAY(trd_exctn_dt);
if _n_=1 then do;
if wd>3 then call symput('e',7-(wd-3));
if wd<3 then call symput('e',3-wd);
if wd=3 then call symput('e',0);
end;
run;

/*ɾ����ĩ*/
data br_2_test1_&i;
set br_2_test1_&i;
%put &e;
if _N_<=&e then delete;
if wd=6 or wd=7 then delete;
run;


/*�������ɵ�br_2_test2_&i���ݼ�����������Ҫ��Ľ��׼۸񣬵����޽���������*/
data br_2_test1_&i;
set br_2_test1_&i;
rename trd_exctn_dt=trade_date;
rename rptd_pr=price;
run;

data br_2_test1_&i(keep=cusip_id trade_date price);
set br_2_test1_&i;
run;

/*7.17 modify,��֤ÿ�����һ�ʽ��׼۸���ǰֵ��ͬ*/
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

step3��ͳ��ÿ������ʵ���ڵĽ�������������ͳ��

*****/
/*���������˲���cound_day_a�����Լ���ȱʧ���ݵ�����������Ӷ�ȷ��������ʵ��������*/
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


/*��ÿ������ʼ����һ��*/
data br_3_&i;
set br_3_&i;
wd=WEEKDAY(trd_exctn_dt);
if _n_=1 then do;
if wd>3 then call symput('e',7-(wd-3));
if wd<3 then call symput('e',3-wd);
if wd=3 then call symput('e',0);
end;
run;

/*��������ĩ��ɾ����ĩ*/
data br_3_&i;
set br_3_&i;
%put &e;
if _N_<=&e then delete;
if wd=6 or wd=7 then delete;
run;


/*�ô�������Ϊ��ͳ�ƽ�����ȱʧ���죬��û�н��׵����ڱ����Ϊ1*/
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

/*ȡ����Ϊһ�����ڣ����Ҽ�����һ�����м���ȱʧ����ͳ��Ϊcount_5����count_5=4���ʾ������4���޽���*/
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

step4��������һ�����ж�Ƚ��׼۸�ʱ�����������һ�ʽ��׼۸��ɱ��ܽ��׼۸��ƽ��ֵ���

****/
/*���¼���count_5Ϊ�н��׵��������������ݼ�br_4_&i��count_5=4��ʾһ������4���н��ף�Ȼ��ͳ��ÿ�ܵ�ƽ�����׼۸���Ϊ�������һ�ʽ��ף���Ϊrptd_pr_5*/
data br_4_&i;
set br_3_&i;
count_5=5-count_5;

if rptd_pr_5^=0 then do;
rptd_pr_5=rptd_pr_5/count_5;
entrd_vol_qt_5=entrd_vol_qt_5/count_5;
end;
run;

/*�ô����Ĳ�Ϊ���ȱʧ��һ�ܵ����ݣ������������н����м�һ��ȱʧ�����������������������������Ϊǰ���������ݵ�ƽ��*/
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

step5��������ȱʧ�

****/
/*���ݼ�br_5_&i ��϶������ݣ�ʵ�ֶԶ���ȱʧ�����ݵĲ���*/
data br_5_&i;
merge br_4_&i br_4_3_&i;
by id;
run;
 
/*�������Ҫ����ܵ�����*/
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

step6�����ȫ�������������ʼ����bond_excess_return��5���ӻع����Q

****/
/*������յ��������������ȱʧ���������ܾ�ֵ�������ȱʧ*/
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


/*7.21����*/
/*
 dm �����˴ӵ�һ�ʽ��׿�ʼ�����״θ�Ϣ�����������
 dw ��������Щ�������ڵ���
 absȡ����ֵ�����ڼ���׼ȷ������
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




/*��������AIT����ծȯӦ����Ϣ */
%interests(br_5_test1_&i,&i);

/*��������coupon���ǽ�������ʱ���coupon��Ϊ0*/
data br_5_test1_&i(drop=p q);
set br_5_test1_&i;
if A=0 then cou=coup;
else if A^=0 then cou=0;
e=rptd_pr+A; /* e Ϊpit+ait�ĺ�*/
e_1=lag(e);
dif=dif(e);
r=(dif+cou)/e_1;
run;


/**�Ե�һ�ν���ǰ�����������������dwֵ
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


/*7.21����*/
/*ƥ��3���Ӳ���*/
proc sql;
create table br_6_&i as
select * from br_5_test1_&i as a left join A.F_f as b on a.trd_exctn_dt-b.dat=0 or a.trd_exctn_dt-b.dat=1 or a.trd_exctn_dt-b.dat=2 or a.trd_exctn_dt-b.dat=3 or a.trd_exctn_dt-b.dat=4;
quit;

/*ƥ��term����*/
proc sql;
create table br_7_&i as
select * 
from br_6_&i as a left join A.lgbt_ombill as b on a.dat-b.ddate=0 or a.dat-b.ddate=1 or a.dat-b.ddate=2 or a.dat-b.ddate=3;
quit;

/*ƥ��def����*/
proc sql noprint;
create table br_8_&i as
select *
from br_7_&i as a left join A.def2 as b on a.id=b.id;
quit;


/*����bond excess return
r-rf = bond excess return
RF�ѳ���0.01*/
data br_8_&i;
set br_8_&i;
rf=RF*0.01;
r_rf=r-rf;
run;

data br_8_&i;
set br_8_&i;
r_rf=r_rf*100;
run;




/*linear reg for f-f3 ���ڼ��㰬����Qֵ*/
/*����ȫ�µ�ff5���Ӽ���Qֵ*/
%reg(br_8_&i);

/*�˴��Ĵ���������ಿ��������-�����ݼ�Ϊ�յ����,�����Ǵ���QֵΪ�ַ���������ֵ�����ܵ��������г���*/
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
reg��������ʵ����
1.�����ݼ�ds�����Իع飬�ؾ�Intercept��������Ϊ������Ҫ��Qֵ

ds:���ڼ���ع�����ݼ�
*********************/
%macro reg(ds);

/*�����ӻع飬������Ҫע��������ƣ�def��term�Ƿ���ȷ*/
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
interests��������ʵ����
1.�������ݼ�ds�Ĳ�ͬ����Ƶ�ʣ����Ǽ����Ӧ��Ӧ����ϢAIT

ds:���ڼ�����Ϣ�����ݼ�
j:��j��ծȯ
******************/
%macro interests(ds,j);

data _null_;
set &ds;
call symput("fre",interest_frequency);
run;


/*freΪ��Ϣ֧����Ƶ�� ����m��ÿm����Ҫ֧��һ����Ϣ*/
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

/*m�������� ÿm�ܽ���һ����Ϣ����ʱcouponΪ��coupon/fre��AITΪ0.
  per���ڹ۲���Ϣ���mod(d_2,m)�����˾�����һ��������m���죬�������Ժܺõķ�Ӧ�������
  �ٳ���m��������ڼ���AIT*/
data br_5_test1_&j;
set br_5_test1_&j;
if &m=0 then per=0;
else per=(mod(d_w,&m))/&m;

/*������ʵ��coupon*/
if &fre=0 then coup=coupon;
else coup=coupon/&fre;

/*AIT*/
if per=0 then A=0;
else A=round(coup*per,0.001);
run;


%mend  interests;



/***************************
calculate��������ʵ����
1.��ȱʧ���ݵ�ǰֵ���ǣ���ȱʧ�ܻ������ծȯ�۸���ǰֵ������������ʼ�۸�$90,��������10���޽��ף���۸񽫰���ǰֵ����ʼ�۸�$90�����䡣
2.����ÿ�콻�׼۸�Ĳ��������������������������rsj, rkt, rsk, rovl�Լ�residual��

n:��n��ծȯ
**************************/
%macro calculate(n);

/****

step1


****/
/*�˴���������ڼ���ÿһ���ƽ���۸�price ���ܳɽ���quantity�����ں���ʵ�������*/
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

step2�������


****/
/*�䷽��Ϊ��������ĳ����ծȯ���׵���������ǲ���ֱ�ӽ��޽��׵�������ծȯ�۸�����Ϊ0�����ǽ�����ծȯ�۸�����Ϊǰֵ�۸�Ŀ��Ϊ��֤�޽���=�޼۸񲨶�*/
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

/*��ÿ����Ϊ��ʼ��һ�ܹ��������콻����*/
data Ds_4_&n;
set Ds_4_&n;
wd=WEEKDAY(trd_exctn_dt);
if _n_=1 then do;
if wd>3 then call symput('e',7-(wd-3));
if wd<3 then call symput('e',3-wd);
if wd=3 then call symput('e',0);
end;
run;

/*��������ĩ��ɾ����ĩ*/
data Ds_4_&n;
set Ds_4_&n;
%put &e;
if _N_<=&e then delete;
if wd=6 or wd=7 then delete;
run;


/****

step3����������


****/
/*
����ÿ��ÿ�ʽ��ײ�ֵ

1.dif1�����ÿ�ս��ײ��rt
2.dif2=rt��ƽ��
3.dif3=rt������
4.dif4=rt���Ĵη�
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


/*ÿ����Ϊһ�ܣ������ܻ���rt�ĺͣ�ƽ���ͣ����η����Լ��Ĵη���*/
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

/*����ѡȡrt>0��rt<0�Ĳ�ͬ���֣����ֽ��ײ����ķ���������Ϊ�˼���rsj����*/
data ds_7p_&n ds_7n_&n;
set ds_5_&n;
if dif1>=0 then output ds_7p_&n;
if dif1<0 then output ds_7n_&n;
run;

/*
  rt>0���ֵ�ƽ����pos
  rt<0���ֵ�ƽ����neg
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

/*7.21�޸�*/
/*�������rsj,rsk,rkt,rovl*/
data ds_9_&n(keep=cusip_id rsj rsk rkt rovl id);
	merge ds_6_&n(keep=w dif1_5 dif2_5 dif3_5 dif4_5)
		  ds_8_&n;
	by w;

	if dif1_5='.' then delete;
	sj_t= rvt_pos-rvt_neg;
	rsj=sj_t/dif2_5;
	/*7.21�޸�*/
	rsk=((sqrt(5))*dif3_5)/(sqrt(dif2_5**3));
	rkt=(5*dif4_5)/dif2_5**2;
	/*7.21�޸�*/
	rovl=(252/5)*sqrt(dif2_5);
	id=_n_;
run;

/*
rsj->rsk �Ĳв������tt_&n�У����Ϊresid_rsj_rsk
rsk->rsj �Ĳв������t_&n�У����Ϊresid_rsk_rsj
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



/*���п�ֵ��������Ϊ0*/
%nullo(ds_9_&n);


proc datasets lib=work  nolist;
save br_8_&n ds_9_&n / memtype=data;
quit;



%mend calculate;


/**************************
nullo()����ʵ����
1.ȷ��ds���ݼ����޿�ֵ�����п�ֵ��0��������ں����������
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
merge()����ʵ����
1.������ݼ�br_8_i��ds_9_i,�����ܻ��İ���bond excess return���ݼ��Ͱ���������������ݼ������һ�𣬱��������ں��������У����ݲ�����С�Ĳ�ͬ��ͳ��return��Qֵ

m:��m��ծȯ
**************************/
%macro merge(m);

proc sql;
create table b_r_&m as
select * from br_8_&m as a left join ds_9_&m as b on a.id=b.id;
quit;

/*�������߼���B*/
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
