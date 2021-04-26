-- 1:
--create table Bikes(bike integer, rent_time timestamp, return_time timestamp);
--
--select * from Bikes;
--
--insert into Bikes values(1, '2021-04-23 08:00:00-00', '2021-04-23 08:30:00-00');
--insert into Bikes values(1, '2021-04-23 08:55:00-00', '2021-04-23 09:15:00-00');
--insert into Bikes values(1, '2021-04-23 09:45:00-00', '2021-04-23 10:30:00-00');
--insert into Bikes values(1, '2021-04-23 11:15:00-00', '2021-04-23 13:45:00-00');


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
--create table emp_register(Employee_ID varchar(2),event_type varchar(3), event_time timestamp);
--insert into emp_register values ('A','IN','2021-04-23 06:00:00-00');
--insert into emp_register values ('A','OUT','2021-04-23 07:14:00-00');
--insert into emp_register values ('B','IN','2021-04-23 07:25:00-00');
--insert into emp_register values ('C','IN','2021-04-23 08:45:00-00');
--insert into emp_register values ('B','OUT','2021-04-23 10:01:00-00');
--insert into emp_register values ('D','IN','2021-04-23 10:30:00-00');
--insert into emp_register values ('A','IN','2021-04-23 10:32:00-00');
--insert into emp_register values ('B','IN','2021-04-23 11:05:00-00');
--insert into emp_register values ('C','OUT','2021-04-23 12:00:00-00');
insert into emp_register values ('E','IN','2021-04-23 10:29:00-00');
--
--select * from emp_register;



--------------
create table dept(  
  deptno     integer,  
  dname      varchar(14),  
  loc        varchar(13),  
  constraint pk_dept primary key (deptno)  
)

create table emp(  
  empno    integer,  
  ename    varchar(10),  
  job      varchar(9),  
  mgr      integer,  
  hiredate date,  
  sal      integer,  
  comm     integer,  
  deptno   integer,  
  constraint pk_emp primary key (empno),  
  constraint fk_deptno foreign key (deptno) references dept (deptno)  
)

insert into DEPT (DEPTNO, DNAME, LOC)values(10, 'ACCOUNTING', 'NEW YORK');
insert into dept  values(20, 'RESEARCH', 'DALLAS');
insert into dept  values(30, 'SALES', 'CHICAGO');
insert into dept  values(40, 'OPERATIONS', 'BOSTON');


insert into emp  values(   7839, 'KING', 'PRESIDENT', null,   '1981-11-17',   5000, null, 10  );
insert into emp  values(   7698, 'BLAKE', 'MANAGER', 7839,   '1981-01-05',   2850, null, 30  );
insert into emp  values(   7782, 'CLARK', 'MANAGER', 7839,   '1981-09-06',   2450, null, 10  );
insert into emp  values(   7566, 'JONES', 'MANAGER', 7839,   '1981-02-04',   2975, null, 20  );
insert into emp  values(   7788, 'SCOTT', 'ANALYST', 7566,   '1987-08-12',   3000, null, 20  );
insert into emp  values(   7902, 'FORD', 'ANALYST', 7566,   '1981-12-03',   3000, null, 20  );
insert into emp  values(   7369, 'SMITH', 'CLERK', 7902,   '1980-12-17',   800, null, 20  );
insert into emp  values(   7499, 'ALLEN', 'SALESMAN', 7698,   '1981-02-20',   1600, 300, 30  );
insert into emp  values(   7521, 'WARD', 'SALESMAN', 7698,   '1981-02-22',   1250, 500, 30  );
insert into emp  values(   7654, 'MARTIN', 'SALESMAN', 7698,   '1981-09-28',   1250, 1400, 30  );
insert into emp  values(   7844, 'TURNER', 'SALESMAN', 7698,   '1981-08-09',   1500, 0, 30  );
insert into emp  values(   7876, 'ADAMS', 'CLERK', 7788,   '1987-04-13', 1100, null, 20  );
insert into emp  values(   7900, 'JAMES', 'CLERK', 7698,   '1981-12-03',   950, null, 30  );
insert into emp  values(   7934, 'MILLER', 'CLERK', 7782,   '1982-01-23',   1300, null, 10  );



---https://www.hackerrank.com/challenges/occupations/problem
create table OCCUPATIONS (name varchar(14), occupation        varchar(14));
  
 insert into OCCUPATIONS values('Samantha','Doctor');
 insert into OCCUPATIONS values('Julia','Actor');
 insert into OCCUPATIONS values('Maria','Actor');
 insert into OCCUPATIONS values('Meera','Singer');
 insert into OCCUPATIONS values('Ashely','Professor');
 insert into OCCUPATIONS values('Ketty','Professor');
 insert into OCCUPATIONS values('Christeen','Professor');
 insert into OCCUPATIONS values('Jane','Actor');
 insert into OCCUPATIONS values('Jenny','Actor');
 insert into OCCUPATIONS values('Priya','Singer');

