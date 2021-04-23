 1) How do you find the Nth percentile
		select
		max(case when a.rownum/a.cnt<=0.9 then a.marks else null) as Nth_percentile
		select 
		(
			select
			marks,
			row_number() (over order by marks) as row_num
			from 
			table
		) a
		cross join
		(
		select
		count(*) as cnt
		from 
		table
		) b
		where 
		1=1

2) How to find the median.

		select
		avg(marks)
		select 
		(
			select
			marks,
			row_number() (over order by marks) as row_num
			from 
			table
		) a
		cross join
		(
		select
		count(*) as cnt
		from 
		table
		) b
		where
		row_num  between (cnt+1)/2 and (cnt+2)/2

3) How to find sum of sales by Category and sub category.

Table:

Customer ID, Product ID, Category ID, sub Category ID, Sales
kartik, P001, C001, S001, 1000

Output:

Customer ID, Categorgy ID, Sub Category ID, Sales, Sales by Category, Sales by Sub Category.

		Select
		customer_id,
		category_id,
		sub_category_id,
		sales,
		sum(sales) over(partition by category_id) as sales_by_category,
		sum(sales) over(partition by sub_category_id) as sales_by_sub_category
		from
		Table;
		
4) Given below data.

Table:

Order_id, order_date, sales

output:

order_date,sales, sales_begining_of_month_to_current_date

		select
		order_date,
		sum(sales) over (partion by order_date) as sales_current_date_sales,
		sum(sales) over(partion by month_begin_date order by order_date asc rows between current_row and unbounded preceeding) as sales_month_to_date
		from
		table1 a,
		dates_table b
		where
		a.order_date=b.calendar_date;

5) Given below data find the manager of the employee, if there is no manager, print "no manager"

Employee ID, employee Name, manager_id

		select
		a.employee_name,
		case when b.employee_id is null then "No Manager" else b.employee_name end as Manager_name
		from
		Table1 a
		left join
		Table1 b
		on b.employee_id=a.manager_id

6) Given the below data, find how many employees where in the building at 10:30 AM

Employee_ID, event_type, event_time,
A    IN        06:00
A    OUT       07:14
B    IN        07:25
C    IN        08:45
B    OUT       10:01
D    IN        10:30
A    IN        10:32
B    IN        11:05
C    OUT       12:00

		select
		employee_id
		from
		(
			select 
			employee_id,
			in_time,
			out_time
			from 
			(
				select
				employee_id,
				event_type,
				event_time as in_time
				lead(event_time) over(partition by employee_id order by event_time asc) as out_event_time
				from
				table1
			) T1
			where T.event_type = 'IN'
		) T2
		where to_date('2015-10-05 10:30:00') between in_time and out_time;
		
7) Write ETL for SCD2

Source working Table: source_table_w
Target Table: target_table_d

		create temp table new_records_v as (
		select
		a.*
		from source_table_w a
		left join
		target_table_d b
		on a.col1=b.col1 
		where b.col1 is null
		);

		create temp table existing_records_v as (
		select
		a.*
		from source_table_w a
		inner join
		target_table_d b
		on a.col1=b.col1 
		);

		update target_table_d 
		from existing_recors_v a
		set end_date=current_timestamp
		where a.col1=col1;

		insert into target_table_d
		select
		a.col1
		curent_date as start_date,
		'2099-12-31' as end_date
		from
		existing_recors_v a
		union
		select
		a.col1
		curent_date as start_date,
		'2099-12-31' as end_date
		from
		new_records_v a

8) Write the ETL for SCD3 

example for SCD3:

customer_id, first_order, last_order

		create temp table first_order_v as (
			select
			customer_id,
			order_id,
			order_date
			from
			(
				select
				customer_id,
				order_id,
				order_date,
				row_number() over(partition by customer_id order by order_date asc) as row_num
				from
				orders_table
			) T
			where T.row_num=1
		);

		create temp table last_order_v as (
			select
			customer_id,
			order_id,
			order_date
			from
			(
				select
				customer_id,
				order_id,
				order_date,
				row_number() over(partition by customer_id order by order_date desc) as row_num
				from
				orders_table
			) T
			where T.row_num=1
		);

		select
		a.customer_id,
		a.order_id as first_order_id,
		a.order_date as first_order_date,
		b.order_id as last_order_id,
		b.order_date as last_order_date
		from
		first_order_v a
		inner jon
		last_order_v b
		on a.customer_id=b.customer_id;

9) Write ETL for late arriving dimensions.

Dimension table: products_table
Fact_table: target_fact_table
late_arriving_temporary table: late_fact_table
source working table: source_table_w

		insert into target_fact_table
		select
		coalesce(b.product_id,-9999) as product_id,
		a.metric_data
		from
		sourc_table_w a
		left join
		products_table b
		where
		a.product_name=b.product_name;

		insert into target_fact_table
		select
		b.product_id as product_id,
		a.metric_data
		from
		late_fact_table a
		inner join
		products_table b
		where
		a.product_name=b.product_name;

		insert into target_fact_table
		select
		-999 as product_id,
		(-1)* a.metric_data as metric_data
		from
		late_fact_table a
		inner join
		products_table b
		where
		a.product_name=b.product_name;

		delete from late_fact_table
		where product_name in (select product_name 
		from
		late_fact_table a
		inner join
		products_table b
		where
		a.product_name=b.product_name);

		insert into late_fact_table
		select
		a.product_name,
		a.metri_data
		from
		source_table_w a
		left join
		product_table b
		on a.product_name=b.product_name
		where b.product_name is null;
		
10) Given the below data, find the 2 actors (male and female both) who have worked together in 2 or more movies.

actor_id, actor_name, actor_gender, movie_id, movie_name
101, kartik, M, M01, ghayal
102, katrina, F, M01, Ghayal

		create temp table male_actor_v as (
		select
		actor_id,
		actor_name,
		movie_id,
		movie_name
		from
		actors where actor_gender='M'
		);

		create temp table female_actor_v as (
		select
		actor_id,
		actor_name,
		movie_id,
		movie_name
		from
		actors where actor_gender='F'
		);

		select
		a.actor_name as male_actor,
		b.actor_name as female_actor,
		count(*) as cnt
		from
		male_actor_v a
		inner join
		female_actor_v b
		where
		a.movie_id=b.movie_id
		group by 1,2
		having count(*)>=2;
		
11) Find the employees whose salary is more than the average sales in their respective departments.

employee_id, employee_name, department_id, salary

		select
		a.employee_id,
		a.employee_name,
		a.department_id,
		a.salary,
		b.avg_dept_salary
		from
		employee_table a,
		(
		select
		department_id,
		avg(salary) as avg_dept_salary
		from employee_table
		) b
		where
		a.department_id=b.department_id 
		and a.salary>b.avg_dept_salary;
		
12) Find the 2nd highest Salary.

		select
		max(a.salary)
		from
		employee_table a
		(
		select
		max(salary) as max_salary
		from employee_table ) b
		where a.salary<max_salary

13) Given the below data, find the following. Students are allowed to take either of 2 subjects i.e. maths or science

Student_id, student_name, subject_id, subject_name

	Find the students who have taken only Maths or Science and NOT both.
	
	select
	a.student_id,
	a.student_name
	from
	(
	select
	student_id,
	student_name,
	sum(case when subject_name='Maths' then 1
		 when subject_name='Science' then 1
		 else 0 end) as flag_cnt
	from
	students_table
	) a
	where flag_cnt=1;
	
	Find the students who have taken both Maths and Science.
	
	select
	a.student_id,
	a.student_name
	from
	(
	select
	student_id,
	student_name,
	sum(case when subject_name='Maths' then 1
		 when subject_name='Science' then 1
		 else 0 end) as flag_cnt
	from
	students_table
	) a
	where flag_cnt=2;
	
14) Lets say we have two tables, one maps product to color
and the other table maps stores to products.

SAMPLE DATA:
TABLE 1          TABLE 2
---------------  --------------
PRODUCT COLOR    STORE PRODUCT
A       YELLOW   1     A
A       BLUE     1     C
B       ORANGE   1     E
C       YELLOW   2     A
D       BLUE     2     B
D       BLACK    3     D
E       YELLOW   3     E



How many stores have yellow color products?

Which store has the most number of colors of any stores?

What is the maximum number of colors any store carries?

select count(*) from (
select
store_id
from
table1 a,
table2 b
where
a.product=b.product and color='YELLOW'
group by 1)

select
store_id
from
(
	select
	store_id,
	row_number() over(order by color_cnt desc) as row_num
	(
		select 
		store_id, 
		count(*) over(partition by store_id) as color_cnt
		from 
		(
			select
			store_id,
			color
			from
			table1 a,
			table2 b
			where
			a.product=b.product
			group by 1,2
		) T1
	) T2
) where T2.row_num=1;

15) There are 2 tables with entries of Apples and Oranges sold each day. Write a query to get difference between apples and oranges sold each day.

Fruit, sale_date, sales_amount
Apple, Sep 1st 2015, 100
oranges, sep 1st 2015, 200

		select
		a.sale_date,
		a.sales_amt-b.sales_amt as diff_amt
		from
		(
		select
		sale_date,
		sales_amt
		from
		fruits_table
		where fruit='Apples'
		) a,
		(
		select
		sale_date,
		sales_amt
		from
		fruits_table
		where fruit='Oranges'
		) b
		where
		a.sale_date=b.sale_date;
		
16) Given the below data, flatten out the data in one row.

Cusomer_id, event_type, event_time,
101, 'UBER BOOKED', Sep 1st 10:00 AM
101, 'UBER Confirmed', Sep 1st 10:05 AM
101, 'UBER Arrived', Sep 1st 10:10 AM
101, 'UBER Dropped', Sep 1st 10:30 AM

		select
		customer_id,
		max(case when event_type='UBER BOOKED' then event_time end) as Uber_booked,
		max(case when event_type='UBER Confirmed' then event_time end) as Uber_Confirmed,
		max(case when event_type='UBER Arrived' then event_time end) as Uber_Arrived,
		max(case when event_type='UBER Dropped' then event_time end) as Uber_Dropped
		from
		Uber_table
		group by 1;

17) Difference between UNION, UNION ALL, INTERSECT

UNION -- eliminates duplicates while used in query. if above and below query between union have the same record only 1 will appear in resultset. Even if one of the queries in have a record it will appear. Can be inefficient as duplicate check is done.
UNION ALL -- Does not do the duplicates check and will result in data from both queries between union all. Efficient as no duplicate check is required
INTERSECT -- Will only display those records who are present in the resultset of above and below query. Records which are present in only one of the queries will not be displayed.

18) How to you replicate row number without using the row_number() window function

Contemporary SQL:

		select
		name
		row_number() over(order by name asc) as row_num
		from
		table1;
		

Without row_number() window function:

		select
		a.name,
		(select count(*) from table1 b where a.name>=b.name) as row_num
		from
		table1 a;
		
19) Given the below data, find out the number of tasks and the time taken to finish every task.

		create table sayee_v
		(
		start_date date,
		end_date date
		)

		insert into sayee_v values('2016-09-01','2016-09-02');
		insert into sayee_v values('2016-09-02','2016-09-03');
		insert into sayee_v values('2016-09-03','2016-09-04');

		insert into sayee_v values('2016-09-05','2016-09-06');
		insert into sayee_v values('2016-09-06','2016-09-07');
		insert into sayee_v values('2016-09-07','2016-09-08');

		insert into sayee_v values('2016-09-10','2016-09-11');
		insert into sayee_v values('2016-09-11','2016-09-12');

		select
		min(start_date) as start_date,
		max(end_date) as end_date,
		max(end_date)-min(start_date) as diff
		from
		(
			select
			start_date,
			end_date,
			row_number() over(order by end_date) as row_num,
			end_date - row_number() over(order by end_date) as tmp
			from sayee_v
		) t
		group by t.tmp;

20) Given the below table, give me the list of all objects which are associated with all tags.

Objects table - has object ID and object name.
Tags Table: has tag ID and Tag name
Bridge table: mapping relation between tag and objects

		select
		T1.object_id,
		T1.object_name
		from
		(
			select
			a.object_id,
			a.object_name,
			count(*) as tag_count
			from
			objects a,
			tags b,
			bridge c
			where
			a.object_id=c.object_id and
			b.tag_id=c.tag_id
		) T1,
		(
			select
			count(*) as tag_count
			from
			tags
		) T2
		where
		T1.tag_count=T2.tag_count;
		
21) Given thebelow tables, find the list of authors for which the books count dont match with the books table.

			Books
           ------------------
           Book_id (PK)
           book_name
           Author_id (FK to authors.author_id)
           published_date
           number_of_words
           number_pages
           number_of_sales

           Authors
           -------------------
           Author_id (PK)
           author_name
           Number_of_books_written


		   select T1.autho_id
		   (
		   select
			author_id,
			count(*) as book_cnt
			from
			books
			) T1,
			authros T2
			where T1.author_id=T2.author_id and
			T1.book_cnt<>T2.number_of_books_written;

			select * from (
			select 
			T1.autor_id 
			case when T2. autorr__id is not null and T1.book_cnt<> T2.Number_of_books_written then ‘NOT matching’ 
			when T2.author_id is null “NOt matching”
			else ‘Mching’ 
			End as tmp

			from
			(select
			author_id,
			count(*) as book_cnt
			from
			books
			group by 1
			) T1
			left join 
			authors T2
			on T1. author_id=T2.auhor_id 
			where
			T1.author_id=T2.author_id )  Fnl where fnl.tmp in “NoT matchin”

21) Give me the list of all the subordinates under one manager. Say a VP, give me all his subordinates.


			select T1.employee_id, T1.employee_name  from (
			select
			employee_id,
			employee_name,
			case when b.employe_id is null then “no manager” else b.emloyeename end as “manager_name”
			from
			employees a
			left jin 
			employees b
			on b.employee_id=a.employee_supervisor
			 ) T1 where T1.manager_name =’Jorie Jhonson”
			 
			 recursive
			 
			 with emp_temp(employee_id,employee_name,manager_id, manager_name)
			 (
			 select employee_id, employee_name,manager_id, employee_name
			 from
			 employees where employee_id=manager_id
			 union all
			 select
			 a.employee_id, a.employee_name, a.manager_id, b.employee_name as manager_name
			 from
			 employees a,
			 emp_temp b
			 where
			 a.manager_id=b.employee_id 
			 )
			 select * from emp_temp where manager_name='Jorie Johonson';
			 
22) Given the below data, explore it for every start and end point. meaning replicate the rows.

city start_ip end_ip
seattle 	100		200
portland 	300		400
San Jose	500		700

		select city,explode_col from (
		select
		city,
		case when start_ip=row_rnk-1 then start_ip 
			 when row_rnk<=end_ip then row_rnk
			 else null end as explode_col
		from
		(
		select
		city,
		start_ip,
		end_ip,
		start_ip+sum(1) over(partition by city rows unbounded preceding) as row_rnk
		from
		kartik_v
		cross join
		amzrep_ddl.o_reporting_days
		) a 			 
		) b where b.explode_col is not null;
		
23) Each customer account is managed by  managers. One account may be managed by multiple  managers. The  managers will share revenue for the corresponding period if they manage the same account. 
Each manager has a ratio, start_date, and end_date for the account. The ratio is used to compute the revenue for the date range.
For example , multiple managers manage the same account, manager_i has a ratio: 1/m,  m is the number of managers manage the same account for that period. If the manager does not share the same date range with other managers, the ratio is 1.

Given an input table AccountManagers( manager_id, account_id, start_date,  end_date), 
Compute the ratio for each manager for all date ranges.
The output table definition is
RecomputedRevenueDistributions( manager_id, account_id,start_date,  end_date,   ratio)

Example
input:
manager_id,account_id, start_date,  end_date        
2,         1,         2016-07-10, 2016-07-20        
3,         1,         2016-07-18, 2016-07-25        
3,         2,         2016-08-01, 2016-08-25        
5,         2,         2016-09-20, 2016-09-30

Output:
2, 1, 2016-07-10, 2016-07-17, 1       //ratio is  1/1=1
2, 1, 2016-07-18, 2016-07-20, 1/2        //ratio  1/2
3, 1, 2016-07-18, 2016-07-20, 1/2     //ratio  1/2
3, 1, 2016-07-21, 2016-07-25, 1       //ratio 1.
3, 2, 2016-08-01, 2016-08-25,  1      // ratio 1
5, 2, 2016-09-20, 2016-09-30, 1       // ratio 1
			 
		create temp table explode_data_v as (
		select 
		manager_id,
		account_id,
		start_date,
		end_date,
		explore_data as data_date
		from
		(
			select
			manager_id,
			account_id,
			case when start_date=row_date-1 then start_date 
				 when row_date<=end_date then row_date
				 else null end as explode_data
			from
			(
				select
				manager_id,
				account_id,
				start_date,
				end_date,
				start_date + sum(1) over(partition by account_id,manager_id rows unbounded preceding) as row_date
				from
				table a
				cross join
				system_table b
				where 1=1
			) a
		) b
		where b.explode_data is not null
		);

		create temp table tmp_v1 as (
		select 
		manager_id,
		account_id,
		start_date,
		end_date,
		data_date,
		count(*) over(partition by account_id,data_date) as manager_cnt
		from
		explore_data_v
		);

		create temp table tmp_v2 as (
		select 
		manager_id,
		account_id,
		min(data_date) over(partition by accont_id,manager_id,manager_cnt) as start_date,
		max(data_date) over(partition by accont_id,manager_id,manager_cnt) as end_date,
		manager_cnt
		from
		tmp_v1
		);

		create temp table final_v as (
		select 
		manager_id,
		account_id,
		start_date,
		end_date,
		1/manager_cnt as ratio
		from
		tmp_v1
		group by 1,2,3,4,5
		);

24) There is professor table, cources table and students table. Then there is one fact less fact table which has keys of professor, cources and students. Find the top 10 courses taken by top 10 professors

		create temp table tmp1_v as (
			sselect
			professor_id,
			course_id,
			count(distinct student_id) over(partition by course_id) as num_of_studs_by_course,
			count(distinct course_id) over(partition by professor_id) as num_of_prof_by_course
			from
			factless_fact_table
			);

		create temp table tmp2_v as (
			select
			professor_id,
			course_id,
			num_of_studs_by_course,
			num_of_prof_by_course
			from
			tmp1_v 
			group by 1,2,3,4
			);	

		create temp table tmp3_v as (
			select
			professor_id,
			course_id,
			row_number() over( order by num_of_studs_by_course desc) as row_num_studs_by_course,
			row_number() over( order by num_of_prof_by_course desc) as row_num_prof_by_course
			from
			tmp2_v 
			group by 1,2,3,4
			);	
			
			select
			professor_id,
			course_id
			from
			tmp3_v where row_num_studs_by_course<=10 and row_num_prof_by_course<=10;

25) Given the below table, rank the salary of employees in each depart in descing and rank commission of employees in each dept in ascending

Employee_id, employee_name, Dept_id, salary, commision

		select
		employee_id,
		employee_name,
		dept_id,
		salary,
		commision,
		row_number() over(partition by dept_id order by salary desc) as row_num_salary_rank_by_dept,
		row_number() over(partition by dept_id order by commision asc) as row_num_commision_rank_by_dept
		from
		employees_table
		
26) Given two tables Friend_request (requester_id, sent_to_id, time) Request_accepted (acceptor_id, requestor_id, time) Find the overall acceptance rate of requests
Assumption: It can take upto one week to accept a request.

Solution-1:

create temp table friends_denorm_v as (
select
a.time as frnd_req_sent_time,
a.requester_id,
a.sent_to_id,
b.time as frnd_req_acpt_time
from
friend_request a
left join
request_accepted b
on a.requester_id=b.requester_id and
a.sent_to_id=b.acceptor_id
);

27) Below is the data in the table. Find the rollwing 2 days sum.

drop table kartik_v;
create  table kartik_v (
order_date date,
sales bigint
);

insert into kartik_v values('2016-11-01',100);
insert into kartik_v values('2016-11-02',800);
insert into kartik_v values('2016-11-03',200);
insert into kartik_v values('2016-11-04',100);
insert into kartik_v values('2016-11-05',900);
insert into kartik_v values('2016-11-06',800);
insert into kartik_v values('2016-11-07',400);
insert into kartik_v values('2016-11-08',300);
insert into kartik_v values('2016-11-09',500);
insert into kartik_v values('2016-11-10',400);

select
b.calendar_day,
sum(sales)
from
kartik_v a,
amzrep_ddl.o_reporting_days b
where
a.order_date between b.calendar_day -1 and b.calendar_day
group by 1

28) Second hightest salary in every department

select
a.department_id,
max(salary) as max_salary
from
employee a
(select
department_id,
max(salary) as max_sal
from
employees) b
where 
a.department_id=b.department_id and
a.salary<max_sal
group by 1

select
a.department_id,
max(a.salary) as max_salary
from
employees a
where a.salary <(select max(salary) from employees b where a.department_id=b.department_id
group by 1

28) Given the below table, get all actor who has acted in maximum number of movies.

Movie_id, City, Location_id, Actor_1, Actor_2, Actor_3

select
actor,
sum(cnt) as movies_count
from
(
	select
	actor_1 as actor,
	count(*) as cnt
	from 
	movies
	group by 1

	union all

	select
	actor_2 as actor,
	count(*) as cnt
	from 
	movies
	group by 1

	union all

	select
	actor_3 as actor,
	count(*) as cnt
	from 
	movies
	group by 1
) T1
group by 1
order by sum(cnt) desc;

29) Given a table below which has user_ids of user and their friends in Facebook.  Find the number of friends (mutual friends) whom a user can make friends with.

Table Name: Friends
Column Name: UserID_1,UserID_2

Create table friends
(
userID_1 varchar(100),
userID_2 varchar(100)
);

insert into friends values('Kartik','Asmita');
insert into friends values('Asmita','Kartik');
insert into friends values('Kartik','Ashish');
insert into friends values('Ashish','Kartik');
insert into friends values('Kartik','Alex');
insert into friends values('Alex','Kartik');
insert into friends values('Kartik','Ramesh');
insert into friends values('Ramesh','Kartik');
insert into friends values('Asmita','Vivek');
insert into friends values('Vivek','Asmita');
insert into friends values('Asmita','Ramesh');
insert into friends values('Ramesh','Asmita');
insert into friends values('Asmita','Raj');
insert into friends values('Raj','Asmita');
insert into friends values('Ashish','Alex');
insert into friends values('Alex','Ashish');
insert into friends values('Ashish','Patrick');
insert into friends values('Patrick','Ashish');
insert into friends values('Ashish','Arun');
insert into friends values('Arun','Ashish');
insert into friends values('Alex','Ramesh');
insert into friends values('Ramesh','Alex');
insert into friends values('Alex','Bernado');
insert into friends values('Bernado','Alex');

select
T1.userID_1,
count(*)
from
(
	select
	a.userID_1,
	b.userID_2
	from
	friends a,
	friends b
	where
	a.userID_2=b.userID_1 and
	a.userID_1<>b.userID_2
	minus
	select
	userID_1,
	userID_2
	from
	friends
	group by 1,2
) T1
group by 1;

30) Given a table which has 2 columns seat number and status of a movie hall. Given me the list of 5 consecutive seats in a row which are vacant and which can be booked.

Create table movies
(
seat_num integer,
status varchar(100)
);

insert into movies values(1,'booked');
insert into movies values(2,'Available');
insert into movies values(3,'Available');
insert into movies values(4,'Available');
insert into movies values(5,'booked');
insert into movies values(6,'booked');
insert into movies values(7,'booked');
insert into movies values(8,'booked');
insert into movies values(9,'booked');
insert into movies values(10,'Available');
insert into movies values(11,'booked');
insert into movies values(12,'Available');
insert into movies values(13,'Available');
insert into movies values(14,'Available');
insert into movies values(15,'Available');
insert into movies values(16,'Available');
insert into movies values(17,'Available');
insert into movies values(18,'booked');
insert into movies values(19,'booked');
insert into movies values(20,'Available');
insert into movies values(21,'Available');
insert into movies values(22,'Available');
insert into movies values(23,'booked');
insert into movies values(24,'booked');
insert into movies values(25,'booked');
insert into movies values(26,'booked');
insert into movies values(27,'Available');
insert into movies values(28,'Available');
insert into movies values(29,'Available');
insert into movies values(30,'Available');
insert into movies values(31,'Available');
insert into movies values(32,'booked');

select
rnk_num,
min(seat_num) as start_seat_num,
max(seat_num) as end_seat_num,
count(*) as cnt_seats
(
	select
	seat_num,
	seat_num-row_number() over(order by seat_num asc) as rnk_num
	from
	movies
	where status='Available'
) T1
group by 1 having count(*) >=5;

31) Find the employees who earn more than their managers.

Emp_id, Name, Salary, Mgr_id

select
a.emp_name
a.emp_salary
from
(
	select
	a.name as emp_name,
	a.salary as emp_salary,
	b.name as mgr_name,
	b.salary as mgr_salary
	from
	employee a,
	left join
	employee b
	on 
	a.mgr_id=b.emp_id
) T1
where
T1.emp_salary>mgr_salary;

32) Given an employee and a department table, find the top 3 salaries in every department.

employee table:
emp_id, name, salary, Dept_id

Department Table:
Dept_id, dept_name

select
T1.dept_name,
T1.emp_name,
T1.emp_salary
from
(
	select
	b.dept_name,
	a.name as emp_name,
	a.salary as emp_salary,
	row_number() over(partition by b.dept_id order by a.salary asc) as rnk_sal
	from
	employee a
	inner join
	department b
	on
	a.dept_id=b.dept_id
) T1
where rnk_sal<=3

33) Given a table with emp_id and email_id, delete the rows which ahve duplicate email and only keep on occorence of the email id row.

----+------------------+
| Id | Email            |
+----+------------------+
| 1  | john@example.com |
| 2  | bob@example.com  |
| 3  | john@example.com |

Output:

 Id | Email            |
+----+------------------+
| 1  | john@example.com |
| 2  | bob@example.com  |
+----+------------------+

create temp table unq_email_v as (
select
id,
email
from
(
	select
	id,
	email,
	row_number() over(partition by email order by id asc) as rnk_email
	from
	employee
) T1
where T1.rnk_email=1
);

delete from employee;
insert into empployee
select * from unq_email_v;

34) Given a table weather, find all the IDs with higher temperature comparred to the Yesterdays dates.

+---------+------------+------------------+
| Id(INT) | Date(DATE) | Temperature(INT) |
+---------+------------+------------------+
|       1 | 2015-01-01 |               10 |
|       2 | 2015-01-02 |               25 |
|       3 | 2015-01-03 |               20 |
|       4 | 2015-01-04 |               30 |

Output:

+----+
| Id |
+----+
|  2 |
|  4 |
+----+

select
T1.id
from
(
	select
	id,
	date,
	temperature,
	lag(temperature) over(order by date asc) as yesterday_temp
	from
	weather
) T1
where
T1.temperature> T1.yesterday_temp;

34) 

The Trips table holds all taxi trips. Each trip has a unique Id, while Client_Id and Driver_Id are both foreign keys to the Users_Id at the Users table. Status is an ENUM type of (‘completed’, ‘cancelled_by_driver’, ‘cancelled_by_client’).

+----+-----------+-----------+---------+--------------------+----------+
| Id | Client_Id | Driver_Id | City_Id |        Status      |Request_at|
+----+-----------+-----------+---------+--------------------+----------+
| 1  |     1     |    10     |    1    |     completed      |2013-10-01|
| 2  |     2     |    11     |    1    | cancelled_by_driver|2013-10-01|
| 3  |     3     |    12     |    6    |     completed      |2013-10-01|
| 4  |     4     |    13     |    6    | cancelled_by_client|2013-10-01|
| 5  |     1     |    10     |    1    |     completed      |2013-10-02|
| 6  |     2     |    11     |    6    |     completed      |2013-10-02|
| 7  |     3     |    12     |    6    |     completed      |2013-10-02|
| 8  |     2     |    12     |    12   |     completed      |2013-10-03|
| 9  |     3     |    10     |    12   |     completed      |2013-10-03| 
| 10 |     4     |    13     |    12   | cancelled_by_driver|2013-10-03|
+----+-----------+-----------+---------+--------------------+----------+
The Users table holds all users. Each user has an unique Users_Id, and Role is an ENUM type of (‘client’, ‘driver’, ‘partner’).

+----------+--------+--------+
| Users_Id | Banned |  Role  |
+----------+--------+--------+
|    1     |   No   | client |
|    2     |   Yes  | client |
|    3     |   No   | client |
|    4     |   No   | client |
|    10    |   No   | driver |
|    11    |   No   | driver |
|    12    |   No   | driver |
|    13    |   No   | driver |
+----------+--------+--------+
Write a SQL query to find the cancellation rate of requests made by unbanned clients between Oct 1, 2013 and Oct 3, 2013. For the above tables, your SQL query should return the following rows with the cancellation rate being rounded to two decimal places.

+------------+-------------------+
|     Day    | Cancellation Rate |
+------------+-------------------+
| 2013-10-01 |       0.33        |
| 2013-10-02 |       0.00        |
| 2013-10-03 |       0.50        |
+------------+-------------------+

select
T1.req_date,
sum_cancellation::float/cnt_tot_trip::float as cancellation_rate
from
(
	select
	a.request_at as req_date,
	a.id,
	count(a.id) over(partition by a.request_at order by a.id) as cnt_tot_trip,
	sum(case when a.status='cancelled_by_driver' then 1 else 0) as sum_cancellation
	from
	trips a,
	users b
	where
	a.client_id=b.user_id and
	b.role='Client' and
	b.banned='No'
) T1

35)

The Employee table holds all employees. The employee table has three columns: Employee Id, Company Name, and Salary.

+-----+------------+--------+
|Id   | Company    | Salary |
+-----+------------+--------+
|1    | A          | 2341   |
|2    | A          | 341    |
|3    | A          | 15     |
|4    | A          | 15314  |
|5    | A          | 451    |
|6    | A          | 513    |
|7    | B          | 15     |
|8    | B          | 13     |
|9    | B          | 1154   |
|10   | B          | 1345   |
|11   | B          | 1221   |
|12   | B          | 234    |
|13   | C          | 2345   |
|14   | C          | 2645   |
|15   | C          | 2645   |
|16   | C          | 2652   |
|17   | C          | 65     |
+-----+------------+--------+
Write a SQL query to find the median salary of each company. Bonus points if you can solve it without using any built-in SQL functions.

+-----+------------+--------+
|Id   | Company    | Salary |
+-----+------------+--------+
|5    | A          | 451    |
|6    | A          | 513    |
|12   | B          | 234    |
|9    | B          | 1154   |
|14   | C          | 2645   |
+-----+------------+--------+

Solution:

create temp table temp_v as (
select
id,
company,
salary,
row_number() over(partition by company order by salary asc) as rnk_company,
count(id) over(partition by company order by id) as cnt_company
from
employee
);

select
company
avg(salary) as median
from
temp_v 
where 
rnk_company between (cnt_company+1)/2 and (cnt_company+2)/2
group by 1;

36)

The Employee table holds all employees including their managers. Every employee has an Id, and there is also a column for the manager Id.

+------+----------+-----------+----------+
|Id    |Name 	  |Department |ManagerId |
+------+----------+-----------+----------+
|101   |John 	  |A 	      |null      |
|102   |Dan 	  |A 	      |101       |
|103   |James 	  |A 	      |101       |
|104   |Amy 	  |A 	      |101       |
|105   |Anne 	  |A 	      |101       |
|106   |Ron 	  |B 	      |101       |
+------+----------+-----------+----------+
Given the Employee table, write a SQL query that finds out managers with at least 5 direct report. For the above table, your SQL query should return:

+-------+
| Name  |
+-------+
| John  |
+-------+

Solution:

select
T.mgr_name,
count(emp_name) as emp_cnt
from
(
	select
	a.name as emp_name,
	b.name as mgr_name
	from
	employees a
	left join 
	employees b
	on a.managerid=b.id
) T1
group by 1 having count(*) >=5

37)

The Numbers table keeps the value of number and its frequency.

+----------+-------------+
|  Number  |  Frequency  |
+----------+-------------|
|  0       |  7          |
|  1       |  1          |
|  2       |  3          |
|  3       |  1          |
+----------+-------------+
In this table, the numbers are 0, 0, 0, 0, 0, 0, 0, 1, 2, 2, 2, 3, so the median is (0 + 0) / 2 = 0.

+--------+
| median |
+--------|
| 0.0000 |
+--------+

Solution:

create temp table temp_v1 as (
select
number,
case when star_freq<end_freq then start_freq+1 else null as row_num
from
(
	select
	number,
	0 as start_freq,
	frequency as end_freq
	from
	numbers
) T1,
system_table T2,
on 1=1
);

create temp table temp_v2 as (
select
number,
row_num,
row_number() OVER(order by number asc) as rnk,
count(number) over(order by number asc) as cnt
from
temp_v1
where
row_num is not null
);

select
avg(number)
from
temp_v2
where
rnk between (cnt+1)/2 and (cnt+2)/2;

38)

Table: Candidate

+-----+---------+
| id  | Name    |
+-----+---------+
| 1   | A       |
| 2   | B       |
| 3   | C       |
| 4   | D       |
| 5   | E       |
+-----+---------+  
Table: Vote

+-----+--------------+
| id  | CandidateId  |
+-----+--------------+
| 1   |     2        |
| 2   |     4        |
| 3   |     3        |
| 4   |     2        |
| 5   |     5        |
+-----+--------------+
id is the auto-increment primary key,
CandidateId is the id appeared in Candidate table.
Write a sql to find the name of the winning candidate, the above example will return the winner B.

+------+
| Name |
+------+
| B    |
+------+

Solution:

select
a.name,
count(b.id) as cnt
from
candidate a,
vote b
where
a.id=b.candidateId
group by a.name order by count(b.id) desc limit 1;

39)
Select all employees name and bonus whose bonus is < 1000.

Table:Employee

+-------+--------+-----------+--------+
| empId |  name  | supervisor| salary |
+-------+--------+-----------+--------+
|   1   | John   |  3        | 1000   |
|   2   | Dan    |  3        | 2000   |
|   3   | Brad   |  null     | 4000   |
|   4   | Thomas |  3        | 4000   |
+-------+--------+-----------+--------+
empId is the primary key column for this table.
Table: Bonus

+-------+-------+
| empId | bonus |
+-------+-------+
| 2     | 500   |
| 4     | 2000  |
+-------+-------+
empId is the primary key column for this table.
Example ouput:

+-------+-------+
| name  | bonus |
+-------+-------+
| John  | null  |
| Dan   | 500   |
| Brad  | null  |
+-------+-------+

Solution:

select
a.name,
b.bonus
from
employee a
left join
bonus b
where
a.empid=b.empId 
where b.emp_id is null or b.bonus<1000;

40)

Get the highest answer rate question from a table survey_log with these columns: uid, action, question_id, answer_id, q_num, timestamp.

uid means user id; action has these kind of values: "show", "answer", "skip"; answer_id is not null when action column is "answer", while is null for "show" and "skip"; q_num is the numeral order of the question in current session.

Write a sql query to identify the question which has the highest answer rate.

Example:
Input:
+------+-----------+--------------+------------+-----------+------------+
| uid  | action    | question_id  | answer_id  | q_num     | timestamp  |
+------+-----------+--------------+------------+-----------+------------+
| 5    | show      | 285          | null       | 1         | 123        |
| 5    | answer    | 285          | 124124     | 1         | 124        |
| 5    | show      | 369          | null       | 2         | 125        |
| 5    | skip      | 369          | null       | 2         | 126        |
+------+-----------+--------------+------------+-----------+------------+
Output:
+-------------+
| survey_log  |
+-------------+
|    285      |
+-------------+
Explanation:
question 285 has answer rate 1/1, while question 369 has 0/1 answer rate, so output 285.

Solution:

create temp table temp_v as (
select
question_id,
sum(case when action='answer' then 1 else 0) as cnt_answer,
count(uid) over(partition by question_id order by uid asc) as cnt_question,
from
survey_log
);

select
question_id,
cnt_answer::float*100/cnt_question::float as per_rate
from
temp_v
order by 
cnt_answer::float*100/cnt_question::float DESC LIMIT 1;

41)

The Employee table holds the salary information in a year.

Write a SQL to get the cumulative sum of an employees salary over a period of 3 months but exclude the most recent month.

The result should be displayed by 'Id' ascending, and then by 'Month' descending.

Example
Input

| Id | Month | Salary |
|----|-------|--------|
| 1  | 1     | 20     |
| 2  | 1     | 20     |
| 1  | 2     | 30     |
| 2  | 2     | 30     |
| 3  | 2     | 40     |
| 1  | 3     | 40     |
| 3  | 3     | 60     |
| 1  | 4     | 60     |
| 3  | 4     | 70     |
Output

| Id | Month | Salary |
|----|-------|--------|
| 1  | 3     | 90     |
| 1  | 2     | 50     |
| 1  | 1     | 20     |
| 2  | 1     | 20     |
| 3  | 3     | 100    |
| 3  | 2     | 40     |
Explanation
Employee '1' has 3 salary records for the following 3 months except the most recent month '4': salary 40 for month '3', 30 for month '2' and 20 for month '1'
So the cumulative sum of salary of this employee over 3 months is 90(40+30+20), 50(30+20) and 20 respectively.

| Id | Month | Salary |
|----|-------|--------|
| 1  | 3     | 90     |
| 1  | 2     | 50     |
| 1  | 1     | 20     |
Employee '2' only has one salary record (month '1') except its most recent month '2'.
| Id | Month | Salary |
|----|-------|--------|
| 2  | 1     | 20     |
Employ '3' has two salary records except its most recent pay month '4': month '3' with 60 and month '2' with 40. So the cumulative salary is as following.
| Id | Month | Salary |
|----|-------|--------|
| 3  | 3     | 100    |
| 3  | 2     | 40     |

Solution:

select
id as emp_id,
month as month_id,
sum(salary) over(order by month_id asc rows between unbounded preceding and current_row) as cum_sum
from
employee a,
(select max(month_id) as max_month from employee) b,
where
a.month_id <b.max_month
order by 1 asc, 2 desc;
order by 1 asc, 2 desc;

42)

A university uses 2 data tables, student and department, to store data about its students and the departments associated with each major.

Write a query to print the respective department name and number of students majoring in each department for all departments in the department table (even ones with no current students).

Sort your results by descending number of students; if two or more departments have the same number of students, then sort those departments alphabetically by department name.

The student is described as follow:

| Column Name  | Type      |
|--------------|-----------|
| student_id   | Integer   |
| student_name | String    |
| gender       | Character |
| dept_id      | Integer   |
where student_id is the students ID number, student_name is the students name, gender is their gender, and dept_id is the department ID associated with their declared major.

And the department table is described as below:

| Column Name | Type    |
|-------------|---------|
| dept_id     | Integer |
| dept_name   | String  |
where dept_id is the departments ID number and dept_name is the department name.

Here is an example input:
student table:

| student_id | student_name | gender | dept_id |
|------------|--------------|--------|---------|
| 1          | Jack         | M      | 1       |
| 2          | Jane         | F      | 1       |
| 3          | Mark         | M      | 2       |
department table:

| dept_id | dept_name   |
|---------|-------------|
| 1       | Engineering |
| 2       | Science     |
| 3       | Law         |
The Output should be:

| dept_name   | student_number |
|-------------|----------------|
| Engineering | 2              |
| Science     | 1              |
| Law         | 0              |


Solution:

select
dept_name,
count(student_id) as cnt
from
students a,
department b
where 
a.dept_id=b.dept_id
group by dept_name;

43)

Given a table customer holding customers information and the referee.

+------+------+-----------+
| id   | name | referee_id|
+------+------+-----------+
|    1 | Will |      NULL |
|    2 | Jane |      NULL |
|    3 | Alex |         2 |
|    4 | Bill |      NULL |
|    5 | Zack |         1 |
|    6 | Mark |         2 |
+------+------+-----------+
Write a query to return the list of customers NOT referred by the person with id '2'.

For the sample data above, the result is:

+------+
| name |
+------+
| Will |
| Jane |
| Bill |
| Zack |
+------+

Solution:

select
name
from 
customers where referee_id<>2;

44)

Write a query to print the sum of all total investment values in 2016 (TIV_2016), to a scale of 2 decimal places, for all policy holders who meet the following criteria:

Have the same TIV_2015 value as one or more other policyholders.
Are not located in the same city as any other policyholder (i.e.: the (latitude, longitude) attribute pairs must be unique).
Input Format:
The insurance table is described as follows:

| Column Name | Type          |
|-------------|---------------|
| PID         | INTEGER(11)   |
| TIV_2015    | NUMERIC(15,2) |
| TIV_2016    | NUMERIC(15,2) |
| LAT         | NUMERIC(5,2)  |
| LON         | NUMERIC(5,2)  |
where PID is the policyholders policy ID, TIV_2015 is the total investment value in 2015, TIV_2016 is the total investment value in 2016, LAT is the latitude of the policy holder's city, and LON is the longitude of the policy holder's city.

Sample Input

| PID | TIV_2015 | TIV_2016 | LAT | LON |
|-----|----------|----------|-----|-----|
| 1   | 10       | 5        | 10  | 10  |
| 2   | 20       | 20       | 20  | 20  |
| 3   | 10       | 30       | 20  | 20  |
| 4   | 10       | 40       | 40  | 40  |
Sample Output

| TIV_2016 |
|----------|
| 45.00    |
Explanation

The first record in the table, like the last record, meets both of the two criteria.
The TIV_2015 value '10' is as the same as the third and forth record, and its location unique.

The second record does not meet any of the two criteria. Its TIV_2015 is not like any other policyholders.

And its location is the same with the third record, which makes the third record fail, too.

So, the result is the sum of TIV_2016 of the first and last record, which is 45.

Solution:

select
sum(Tiv_2016)
from
(
	select
	pid,
	Tiv_2015
	count(pid) over(partition by lat,lon order by pid) cnt
	from
	insurance
) T1,
insurance T2
where
T1.cnt=1 and 
T1.Tiv_2015=T2.Tiv_2015;

45)

Query the customer_number from the orders table for the customer who has placed the largest number of orders.

It is guaranteed that exactly one customer will have placed more orders than any other customer.

The orders table is defined as follows:

| Column            | Type      |
|-------------------|-----------|
| order_number (PK) | int       |
| customer_number   | int       |
| order_date        | date      |
| required_date     | date      |
| shipped_date      | date      |
| status            | char(15)  |
| comment           | char(200) |
Sample Input

| order_number | customer_number | order_date | required_date | shipped_date | status | comment |
|--------------|-----------------|------------|---------------|--------------|--------|---------|
| 1            | 1               | 2017-04-09 | 2017-04-13    | 2017-04-12   | Closed |         |
| 2            | 2               | 2017-04-15 | 2017-04-20    | 2017-04-18   | Closed |         |
| 3            | 3               | 2017-04-16 | 2017-04-25    | 2017-04-20   | Closed |         |
| 4            | 3               | 2017-04-18 | 2017-04-28    | 2017-04-25   | Closed |         |
Sample Output

| customer_number |
|-----------------|
| 3               |

Solution:

select
customer_number,
count(order_number) as cnt_orders
from
orders 
group by 1 order by count(order_number) desc limit 1;

46)

In social network like Facebook or Twitter, people send friend requests and accept others’ requests as well. Now given two tables as below:

Table: friend_request
| sender_id | send_to_id |request_date|
|-----------|------------|------------|
| 1         | 2          | 2016_06-01 |
| 1         | 3          | 2016_06-01 |
| 1         | 4          | 2016_06-01 |
| 2         | 3          | 2016_06-02 |
| 3         | 4          | 2016-06-09 |
Table: request_accepted
| requester_id | accepter_id |accept_date |
|--------------|-------------|------------|
| 1            | 2           | 2016_06-03 |
| 1            | 3           | 2016-06-08 |
| 2            | 3           | 2016-06-08 |
| 3            | 4           | 2016-06-09 |
| 3            | 4           | 2016-06-10 |
Write a query to find the overall acceptance rate of requests rounded to 2 decimals, which is the number of acceptance divide the number of requests.
For the sample data above, your query should return the following result.
|accept_rate|
|-----------|
|       0.80|
Note:
The accepted requests are not necessarily from the table friend_request. In this case, you just need to simply count the total accepted requests (no matter whether they are in the original requests), and divide it by the number of requests to get the acceptance rate.
It is possible that a sender sends multiple requests to the same receiver, and a request could be accepted more than once. In this case, the ‘duplicated’ requests or acceptances are only counted once.
If there is no requests at all, you should return 0.00 as the accept_rate.
Explanation: There are 4 unique accepted requests, and there are 5 requests in total. So the rate is 0.80.
Follow-up:
Can you write a query to return the accept rate but for every month?
How about the cumulative accept rate for every day?

Solution:

select
(case when b.requester_id is not null then 1 else 0 end )*100/count(sender_id)
from
friend_request a
left join
request_accepted b
on
a.sender_id=b.requester_id;

Can you write a query to return the accept rate but for every month?

select
T2.month_beg_date as request_month_date,
sum(T1.acpt_num) over(partition by T3.month_beg_date) *100 / count(sender_id) over(partition by T3.month_beg_date) as
from
(
	select
	a.request_date,
	b.accept_date,
	case when b.requester_id is not null then 1 else 0 end as acpt_num,
	a.sender_id
	from
	friend_request a
	left join
	request_accepted b
	on
	a.sender_id=b.requester_id
) T1,
dates T2,
dates T3
where
T1.request_date=T2.date and
T1.accept_date=T2.date;


How about the cumulative accept rate for every day?

select
T2.request_date as date,
sum(T1.acpt_num) over(order by T1.request_date asc rows between unbounded preceeding and current_row) *100 / count(sender_id) over(order by T1.request_date asc rows between unbounded preceeding and current_row) as accept_rate
from
(
	select
	a.request_date,
	case when b.requester_id is not null then 1 else 0 end as acpt_num,
	a.sender_id
	from
	friend_request a
	left join
	request_accepted b
	on
	a.sender_id=b.requester_id
) T1;

47)

X city built a new stadium, each day many people visit it and the stats are saved as these columns: id, date, people

Please write a query to display the records which have 3 or more consecutive rows and the amount of people more than 100(inclusive).

For example, the table stadium:
+------+------------+-----------+
| id   | date       | people    |
+------+------------+-----------+
| 1    | 2017-01-01 | 10        |
| 2    | 2017-01-02 | 109       |
| 3    | 2017-01-03 | 150       |
| 4    | 2017-01-04 | 99        |
| 5    | 2017-01-05 | 145       |
| 6    | 2017-01-06 | 1455      |
| 7    | 2017-01-07 | 199       |
| 8    | 2017-01-08 | 188       |
+------+------------+-----------+
For the sample data above, the output is:

+------+------------+-----------+
| id   | date       | people    |
+------+------------+-----------+
| 5    | 2017-01-05 | 145       |
| 6    | 2017-01-06 | 1455      |
| 7    | 2017-01-07 | 199       |
| 8    | 2017-01-08 | 188       |
+------+------------+-----------+

Solution:

select
id,
date,
people
from
(
	select
	id,
	date,
	people,
	count(*) over(partition by rnk) as cnt
	from
	(
		select
		id,
		date,
		id-row_number() over(order by id asc) as rnk,
		people
		from
		statium
		where people>=100
	) T1
) T2
where T2>=3;

48)

In social network like Facebook or Twitter, people send friend requests and accept others requests as well.

Table request_accepted holds the data of friend acceptance, while requester_id and accepter_id both are the id of a person.
| requester_id | accepter_id | accept_date|
|--------------|-------------|------------|
| 1            | 2           | 2016_06-03 |
| 1            | 3           | 2016-06-08 |
| 2            | 3           | 2016-06-08 |
| 3            | 4           | 2016-06-09 |
Write a query to find the the people who has most friends and the most friends number. For the sample data above, the result is:
| id | num |
|----|-----|
| 3  | 3   |
Note:
It is guaranteed there is only 1 people having the most friends.
The friend request could only been accepted once, which mean there is no multiple records with the same requester_id and accepter_id value.
Explanation:
The person with id '3' is a friend of people '1', '2' and '4', so he has 3 friends in total, which is the most number than any others.
Follow-up:
In the real world, multiple people could have the same most number of friends, can you find all these people in this case?

Solution:

select
user_id,
count(friend) as cnt_friend
from
(
	select
	requester_id as user_id,
	accepter_id as friend
	from
	request_accepted
	union
	select
	accepter_id as user_id,
	requester_id as friend
	from
	request_accepted
) T1
group by 1 order by count(friend) desc limit 1;


49)

Description

Given three tables: salesperson, company, orders.
Output all the names in the table salesperson, who didn’t have sales to company 'RED'.

Example
Input

Table: salesperson

+----------+------+--------+-----------------+-----------+
| sales_id | name | salary | commission_rate | hire_date |
+----------+------+--------+-----------------+-----------+
|   1      | John | 100000 |     6           | 4/1/2006  |
|   2      | Amy  | 120000 |     5           | 5/1/2010  |
|   3      | Mark | 65000  |     12          | 12/25/2008|
|   4      | Pam  | 25000  |     25          | 1/1/2005  |
|   5      | Alex | 50000  |     10          | 2/3/2007  |
+----------+------+--------+-----------------+-----------+
The table salesperson holds the salesperson information. Every salesperson has a sales_id and a name.
Table: company

+---------+--------+------------+
| com_id  |  name  |    city    |
+---------+--------+------------+
|   1     |  RED   |   Boston   |
|   2     | ORANGE |   New York |
|   3     | YELLOW |   Boston   |
|   4     | GREEN  |   Austin   |
+---------+--------+------------+
The table company holds the company information. Every company has a com_id and a name.
Table: orders

+----------+----------+---------+----------+--------+
| order_id |  date    | com_id  | sales_id | amount |
+----------+----------+---------+----------+--------+
| 1        | 1/1/2014 |    3    |    4     | 100000 |
| 2        | 2/1/2014 |    4    |    5     | 5000   |
| 3        | 3/1/2014 |    1    |    1     | 50000  |
| 4        | 4/1/2014 |    1    |    4     | 25000  |
+----------+----------+---------+----------+--------+
The table orders holds the sales record information, salesperson and customer company are represented by sales_id and com_id.
output

+------+
| name | 
+------+
| Amy  | 
| Mark | 
| Alex |
+------+
Explanation

According to order '3' and '4' in table orders, it is easy to tell only salesperson 'John' and 'Alex' have sales to company 'RED',
so we need to output all the other names in table salesperson.

Solution:

select
c.name
from
salesperson c
where not exist 
(
	select
	1
	from
	orders a,
	company b
	where
	a.com_id=b.com_id and b.name='RED'
	and c.com_id=a.com_id
)
group by 1;

50)

Given a table tree, id is identifier of the tree node and p_id is its parent nodes id.

+----+------+
| id | p_id |
+----+------+
| 1  | null |
| 2  | 1    |
| 3  | 1    |
| 4  | 2    |
| 5  | 2    |
+----+------+
Each node in the tree can be one of three types:
Leaf: if the node is a leaf node.
Root: if the node is the root of the tree.
Inner: If the node is neither a leaf node nor a root node.
Write a query to print the node id and the type of the node. Sort your output by the node id. The result for the above sample is:
+----+------+
| id | Type |
+----+------+
| 1  | Root |
| 2  | Inner|
| 3  | Leaf |
| 4  | Leaf |
| 5  | Leaf |
+----+------+
Explanation

Node '1' is root node, because its parent node is NULL and it has child node '2' and '3'.
Node '2' is inner node, because it has parent node '1' and child node '4' and '5'.
Node '3', '4' and '5' is Leaf node, because they have parent node and they dont have child node.

And here is the image of the sample tree as below:
			  1
			/   \
          2       3
		/   \
      4       5
Note

If there is only one node on the tree, you only need to output its root attributes.

Solution:

select
node,
case
	when parent=null then 'Root'
	when child_flg='True' then 'Inner'
	else 'Leaf'
end as Type
from
(
	select 
	a.id as node,
	a.p_id as parent,
	case when b.p_id is not null then 'True' else 'False' end child_flg
	from
	tree a
	left join
	tree b
	on 
	a.id=b.p_id
) T1;

51)

A pupil Tim gets homework to identify whether three line segments could possibly form a triangle.

However, this assignment is very heavy because there are hundreds of records to calculate.
Could you help Tim by writing a query to judge whether these three sides can form a triangle, assuming table triangle holds the length of the three sides x, y and z.
| x  | y  | z  |
|----|----|----|
| 13 | 15 | 30 |
| 10 | 20 | 15 |
For the sample data above, your query should return the follow result:
| x  | y  | z  | triangle |
|----|----|----|----------|
| 13 | 15 | 30 | No       |
| 10 | 20 | 15 | Yes      |


Solution:

select
x,y,z,
case when x+y>=z and x+z>y and y+z>x then 'Yes' else 'No' end Triangle
from
triagle_table;

52)

In facebook, there is a follow table with two columns: followee, follower.

Please write a sql query to get the amount of each follower’s follower if he/she has one.

For example:

+-------------+------------+
| followee    | follower   |
+-------------+------------+
|     A       |     B      |
|     B       |     C      |
|     B       |     D      |
|     D       |     E      |
+-------------+------------+
should output:
+-------------+------------+
| follower    | num        |
+-------------+------------+
|     B       |  2         |
|     D       |  1         |
+-------------+------------+
Explaination:
Both B and D exist in the follower list, when as a followee, B's follower is C and D, and D's follower is E. A does not exist in follower list.
Note:
Followee would not follow himself/herself in all cases.
Please display the result in followers alphabet order.

Solution:

select
followee,
count(*)
from
(
	select
	a.followee,
	a.follower,
	case when b.follower is not null the 'True' else 'False' end flg
	from
	follow_table a
	left join
	follow_table b
	on
	a.followee=b.follower
) T1
where flg='True';

53) 

Given a table which has one column which has only integer values, write a query to find max value from this table without using any of MAX, RANK, Window functions.

Solution:

select col1 from table where col1 not in (
select
a.col1
from
table a,
table b
where a.col1<b.col1
);

Efficient Solution:

select
a.col1
from
table a
left join 
table b
on a.col1>b.col1
group by 1 having count(*)= (select count(*) from table)

54)

Given the below table data, give the number of first time visitors per day.

user_id date
adam	jan 1st
adam	jan 2nd
adam	jan 3rd
kartik	jan 4th
vong	jan 3rd

select 
date,
count(user_id) as cnt
from
(
select
user_id,
min(date) as date
from 
user_table
group by 1
) b
group by 1;

55)

Given the below table data, give the count of visitors who came the previous day and the current day as well

user_id date
adam	jan 1st
adam	jan 2nd
adam	jan 4th
adam 	jan 5th
kartik	jan 4th
kartik jan 5th
vong	jan 3rd

Output:

jan 1st 0
jan 2nd 1
jan 3rd 0
jan 4th 0
jan 5th 2

Solution:

select
first_date as date,
sum(case when first_date-1=second_date then 1 else 0 end) as cnt
(
select
user_id,
date as first_date,
lag(date,1) over(partition by user_id order by date) as second_date
from
user_table
) a
group by 1

	A table schea with tables like employee, department, employee_to_projects, projects

1) Select employee from departments where max salary of the department is 40k
2) Select employee assigned to projects
3) Select employee which have the max salary in a given department
4) Select employee with second highest salary
5) Table has two data entries every day for # of apples and oranges sold. write a query to get the difference between the apples… 

56) Given a Students Table, Assisnments table and grades table. Find out the students whose average grades are greater than 50 percentile of class. While taking out percentile, only take into consderation those students
who have taken all assisnments. do not incclude those students who have not completed an assignment. 

create temp table V1 (
select
a.sudent_id,
a.student_name,
a.assignemt_id
from
students 
cross join
assisgnments
on 1=1
);

create temp table v2 as (
select
a.student_id
from
v1 a
where not exists (select 1 from grades b
on a.student_id=b.student_id
and a.assigment_id=b.assignet_id)
group by a.student_id
);

create temp table v3 as (
select
student_id,
avg(grades) as avg_grades
from
grades where student_id not in (select student_id from v2)
group by student_id
);

create temp table v4 as (
select
student_id,
avg_grades,
row_number() over(order by avg grades asc) as rnk,
T.cnt as student_count
from
v3
cross join
(select count(student_id) as cnt from v3) as T
where
1=1
);

create temp table v5 as (
select
student_id,
avg_grades
from
(
select
student_id,
avg_grades,
case when rnk / student_count >= 0.5 then 'True' else 'False' end percentile
from
v4
) as T
where T.percentile='True'
);

57) There is an attendance table which has student Id ,date which signifies the dates of attendance of the students. Given this table, 
find out those students who did not come to college ONLY on Friday for 2018
but came all the other days.

create temp table v1 as (
select
d.calendar_date,
att.student_id,
d.day_of_week_number,
d.week_beg_date
from
dates d
cross join
(select student_id from attendance group by student_id) att
on
1=1
);

create temp table v2 as (
select
T.student_id,
T.week_beg_date,
sum(case when T.attendance_flg='TRUE' then 1 else 0 end) as cnt
from
(
	select
	a.calendar_date,
	a.student_id,
	a.day_of_week_number,
	a.week_beg_date,
	case 
	when a.day_of_week_number in (1,2,3,4,5) and b.student_id is not null and b.attendance_date is not null then 'TRUE'
	when a.day_of_week_number in (5) and b.student_id is null and b.attendance_date is null then 'FALSE'
	else null 
	end attendance_flg
	from
	v1 a
	inner join
	student_attendance b
	on a.student_id=b.student_id and a.calendar_date=b.attendence_date
) T
T.attendance_flg in ('TRUE','FALSE')
);

create temp table v3 as (
select
student_id
from
v2
where cnt=4
group by student_id
);
