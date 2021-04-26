--1:Bike Problem

Input:
--------
--bike|rent_time|return_time
1|8:00|8:30
1|9:00|9:30
1|11:00|12:00
2|10:00|11:00
2|11:30|12:00

--Output
-- bike|idel_time_in|idel_time_out
-- 1|8:30|9:00
-- 1|9:30|11:00
-- 2|11:00|11:30

select 
bike,
return_time,
lead(rent_time) over (partition by bike order by rent_time asc ) as return_time
from bikes;


--2: Employee Building Record Problem
-- Given the below data, find how many employees where in the building at 10:30 AM
--Employee_ID, event_type, event_time,
--A    IN        06:00
--A    OUT       07:14
--B    IN        07:25
--C    IN        08:45
--B    OUT       10:01
--D    IN        10:30
--A    IN        10:32
--B    IN        11:05
--C    OUT       12:00

update emp_register set event_time = '2021-04-23 10:28:00' where employee_id='E'

select Employee_ID, event_type, in_time, out_time
from (
select 
Employee_ID, event_type, in_time, out_time 
from (
select 
Employee_ID
,event_type
,event_time as in_time
, lead(event_time) over(partition by Employee_ID order by event_time asc) as out_time
from emp_register) T
where T.event_type = 'IN'
)T2
--where (('2021-04-23 10:30:00-00') between in_time and  out_time ) 
where (('2021-04-23 10:30:00-00') between in_time and  '2021-04-23 12:00:00' ) 
--where (T2.in_time:: date > date '2021-04-23 05:00:00') 

--3: Given the below table, rank the salary of employees in each depart 
--in descing and rank commission of employees in each dept in ascending
--
--create table emp(  
--  empno    integer,  
--  ename    varchar(10),  
--  job      varchar(9),  
--  mgr      integer,  
--  hiredate date,  
--  sal      integer,  
--  comm     integer,  
--  deptno   integer,)
select * from emp;


select empno, deptno, sal, comm,
rank() over(partition by deptno order by sal desc ) as rank_sal_by_dept,
rank() over(partition by deptno order by comm asc) as rank_comm_by_dept
from emp;

--4: Find median salary from emp table
select
	avg(sal)
	from
		(
		select * from 
			(
			select
			sal,
			row_number() over (order by sal) as row_num
			from 
			emp
			) a
		cross join
			(
			select
			count(*) as cnt
			from 
			emp
			) b
		) c
		where
		row_num  between (cnt+1)/2 and (cnt+2)/2
		
--4: https://www.hackerrank.com/challenges/occupations/problem
select * from OCCUPATIONS;
-- Input:
--Samantha	Doctor
--Julia	Actor
--Maria	Actor
--Meera	Singer
--Ashely	Professor
--Ketty	Professor
--Christeen	Professor
--Jane	Actor
--Jenny	Actor
--Priya	Singer

--Output:
--Jenny    Ashley     Meera  Jane
--Samantha Christeen  Priya  Julia
--NULL     Ketty      NULL   Maria

Select D.Name, P.Name, S.Name, A.Name
from
(Select Name, row_number() over (partition by occupation order by name) id from Occupations where Occupation = 'Doctor') D
full outer join
(Select Name, row_number() over (partition by occupation order by name) id from Occupations where Occupation = 'Professor') P on D.id = P.id
full outer join
(Select Name, row_number() over (partition by occupation order by name) id from Occupations where Occupation = 'Singer') S on P.id = S.id
full outer join
(Select Name, row_number() over (partition by occupation order by name) id from Occupations where Occupation = 'Actor') A on S.id = A.id;



