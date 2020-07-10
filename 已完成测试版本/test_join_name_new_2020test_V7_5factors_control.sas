/*1.ƥ��term���ӣ�����ɡ�����ɣ�


  2.����def���ӣ����ƥ��
	a.������Ҫ����Ŀǰ�Ĵ������def����һ����Ҫ����10000�������Ҫ���ծȯreturn����
	b.�ڶ�����Ҫ�Լ����ծȯ�������򣬲��Ҽ���ÿ���е�value_weight_return
		I.����˼·����def���е����ļ��㣬�������
		II.�ۺ���ȫ��������һ�����
  ��÷�������������def�����ڼ�����⡣
  �����С�����ɣ�

  3.�����Ӽ�������ԣ�δ��ɣ�

  4.single-sorted portfolios with control  rsj-��rsk�Ѿ���ɣ���ɣ�
  5.������Ҫ���Ǻ�ʱʹ��rsk->rsj�Ĳв����

ps��ע��def������A.def2�У�Ŀǰ����Ҫ���м���ſ�����ȫƥ����
ps: �ð汾���ڷ�control����������Ȼ����



*/
%macro split(ds,ds1);
      
        proc sql noprint;
        select distinct cusip_id into: names separated by ','/*�����������names�����ŷָ�*/
        from &ds;
        quit;
        %let i=1;
        %do %while(%scan(%quote(&names),&i,',') ne %str());/*�Ӵ���Ϊ����ѭ��������ݼ�*/
                %let dname=%scan(%quote(&names),&i,',');
				
				
				data D_&i;
                set &ds1;
                where cusip_id = "&dname";
                run;
				
				proc sql noprint;
				create table Ds_&i as
				select * from D_&i as a left join A.name_info_new as b on a.cusip_id=b.cusip_id; /*name_info_new ���е�couponΪ���¼�����*/
				quit;

				/*ԭʼ�����Ѿ�����Ҫ������*/
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


%macro loop(i);

 %bond_return(Ds_&i,&i);
 %calculate(&i);
 %merge(&i);

/*delete useless*/
/* proc datasets lib=work  nolist;
		delete rm_week_&i w_5_&i / memtype=data;
		quit;
*/

 dm log 'clear;' continue;

%mend loop;



%macro bond_return(ds,i);

/*����ÿһ���ƽ���۸�price ���ܳɽ���quantity ���������ݲ�ȫʱ���׼ȷ��*/
/*���⣺��ȫ������û�����⣬�������ÿ�վ�ֵ��������Ҫ�ҵ�ÿ�����һ�ʽ��ף�������ǲ�ȫ���ݺ���Ҫ��ԭʼ�����ںϣ�����ԭʼ���ݵ��������������һ�ʽ���*/
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


/*V6 ��bond returnҲ��������ʵ�Բ���*/
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

/*��������ĩ��ɾ����ĩ*/
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



/*v4��������*/
/*���������˸����������Լ���ȱʧ���ݵ������*/
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


/*�ô�������Ϊ�˴���ͳ��ȱʧ���죬���Ϊ1*/
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

/*ȡ����Ϊһ�����ڣ����Ҽ�����һ�����м���ȱʧ����ͳ��Ϊcount_5*/
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

/*���¼���count_5Ϊ�н��׵�������Ȼ�����ÿ�ܵ�ƽ����Ϊ�������һ�ʽ��ף���Ϊrptd_pr_5������*/
data br_4_&i;
set br_3_&i;
count_5=5-count_5;

if rptd_pr_5^=0 then do;
rptd_pr_5=rptd_pr_5/count_5;
entrd_vol_qt_5=entrd_vol_qt_5/count_5;
end;
run;

/*�ô����Ĳ�Ϊ���ȱʧ��һ�ܵ����ݣ��������н����м�һ��ȱʧ�����������������������������Ϊǰ���������ݵ�ƽ��*/
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
 
/*����Ҫ����ܵ�����*/
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
/*v4�������ֽ���*/

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


/*dm �����˴ӵ�һ�ʽ��׿�ʼ�����״θ�Ϣ�����������
 dw ��������Щ�������ڵ��ܣ�absȡ����ֵ�����ڼ���׼ȷ������*/
data br_5_test1_&i;
set br_5_test1_&i nobs=nobs;
d_m=first_interest_date-trd_exctn_dt;
d_w=ceil(round(d_m/7,0.001));
d_w=abs(d_w);
run;




/*��������AIT */
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


/*v4����*/

/*����ʱ�����Ȼ��Ҫ����ȷ�ϣ���ȷ��ƥ�����ȷ�� ps ���*/
proc sql;
create table br_6_&i as
select * from br_5_test1_&i as a left join A.F_f as b on a.trd_exctn_dt-b.dat=0 or a.trd_exctn_dt-b.dat=1 or a.trd_exctn_dt-b.dat=2 or a.trd_exctn_dt-b.dat=3 or a.trd_exctn_dt-b.dat=4 or a.trd_exctn_dt-b.dat=5 or a.trd_exctn_dt-b.dat=6;
quit;

/*r-rf bond excess return
RF�ѳ���0.01*/
data br_6_&i;
set br_6_&i;
rf=RF*0.01;
r_rf=r-rf;
run;

data br_6_&i;
set br_6_&i;
r_rf=r_rf*100;
run;

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


/*linear reg for f-f3 ���ڼ��㰬����Qֵ*/
/*����ȫ�µ�ff5���Ӽ���Qֵ*/
%reg(br_8_&i);

/*v4����*/
data br_8_&i;
set br_8_&i;
if entrd_vol_qt=0 then do;
r_rf='.';
Q='.';
end;

run;

/*v5����*/
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




/*delete useless*/

proc datasets lib=work  nolist;
		delete br_1_&i br_2_&i br_2_test1_&i br_2_test2_&i br_3_&i br_4_&i br_5_&i br_5_test1_&i br_6_&i / memtype=data;
		quit;


%mend bond_return;



%macro reg(ds);

/*������Ҫע��������ƣ�def��term�Ƿ���ȷ*/
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

%macro interests(ds,j);

data _null_;
set &ds;
call symput("fre",interest_frequency);
run;


/*fre Ϊ��Ϣ֧����Ƶ�� ����mΪm����Ҫ֧��һ����Ϣ*/
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




%macro calculate(n);
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

/*��������Ҫ���ǲ������ݣ�
����1��k��ƽ��
����2��0ֵ�*/

/*����1*/

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

/*��ÿ������ʼ����һ��*/
/*��������wd=3�����*/
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

/*����ÿ��ÿ�ʽ��ײ�ֵ*/
data ds_5_&n;
set Ds_4_&n;

/*dif1�����ÿ�ս��ײ��rt*/

dif1=dif(rptd_pr);
if wd=3 then dif1=0;
/*dif2=rt��ƽ��*/
dif2=dif1**2;
/*dif3=rt������*/
dif3=dif1**3;
dif4=dif1**4;

num=_n_-1;
w=int(num/5)+1;

drop num;
run;
/*��˽������Ĳ��������汾1��ͬ�����ǽ����±�д����*/

/*ÿ����Ϊһ�ܣ�����rt�ĺͣ�ƽ���ͣ����η����Լ��Ĵη���*/
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

/*����ѡȡrt>0��rt<0�Ĳ�ͬ����*/
data ds_7p_&n ds_7n_&n;
set ds_5_&n;
if dif1>=0 then output ds_7p_&n;
if dif1<0 then output ds_7n_&n;
run;

/*rt>0���ֵ�ƽ����pos
  rt<0���ֵ�ƽ����neg*/
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

/*����ע��rvtΪÿ�ս��ײ��ƽ���ͣ���ζ��rvt=dif2_5.Ҳ����ÿ�����죬�ռ佻�ײ��ƽ�ź�*/
data ds_9_&n(keep=cusip_id rsj rsk rkt rovl id);
	merge ds_6_&n(keep=w dif1_5 dif2_5 dif3_5 dif4_5)
		  ds_8_&n;
	by w;

	if dif1_5='.' then delete;

/*Ŀǰ��ʽ���д�����*/
	sj_t= rvt_pos-rvt_neg;
	rsj=sj_t/dif2_5;
	rsk=((sqrt(5))*dif3_5)/(sqrt(sqrt(dif2_5)**3));
	rkt=(5*dif4_5)/dif2_5**2;
	rovl=sqrt(dif2_5);
	id=_n_;
run;

/*rsj->rsk �Ĳв�
  rsk->rsj �Ĳв�*/
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




%nullo(ds_9_&n);


proc datasets lib=work  nolist;
save br_8_&n ds_9_&n / memtype=data;
quit;



%mend calculate;


%macro filled(ds1,ds2,var1);

data &ds2(rename=(_date=trd_exctn_dt) rename=(_&var1=&var1) );
   set &ds1;
	by cusip_id;
   retain  _date count_day _&var1;
   format _date YYMMDDN8.;
   if first.cusip_id then do;
      count_day=1;
          _date=trd_exctn_dt;
		  _&var1=&var1;
          output;
    end;
     else do;
	 		  
			  
               _date=intnx('day',_date,1);
                  if _date<trd_exctn_dt then do until (_date=trd_exctn_dt);
              count_day=count_day;
			  
			  _&var1=0;
               output;
              _date=intnx('day',_date,1);
                       end;
                 if _date=trd_exctn_dt then do;
                                count_day+1;
								_&var1=&var1;
                                output;
                             end;
     end;

	 drop trd_exctn_dt &var1;

run;


data &ds2;
set &ds2;
wd=WEEKDAY(trd_exctn_dt);
if _n_=1 then do;
if wd>3 then call symput('e',7-(wd-3));
if wd<3 then call symput('e',3-wd);
if wd=3 then call symput('e',3);
end;
run;

data &ds2;
set &ds2;
%put &e;
if _N_<=&e then delete;
if wd=6 or wd=7 then delete;
run;

%mend filled;


%macro nullo(ds);

	data &ds;
	set &ds;
	array numtmp _numeric_;
	do over numtmp;
	numtmp=coalesce(numtmp,0);
	end;
	run;


%mend nullo;



%macro merge(m);

proc sql;
create table b_r_&m as
select * from br_8_&m as a left join ds_9_&m as b on a.id=b.id;
quit;

/*���߼����д��� B*/
data B.b_r_&m;
set b_r_&m;
p_n=rptd_pr*entrd_vol_qt;
if dat=' ' then delete;
run;


proc datasets lib=work  nolist;
delete br_8_&m b_r_&m ds_9_&m / memtype=data;
quit;

%mend merge;

%split(A.name_test,A.Trace_enhanced_clean_cut);
