# **SQL**

## Single Value Sub-Query: Sub-query in the where clasuse of a SQL query

### *Problem 1. Find highest earning salary*

select * from employee where salary =
(select max(salary) from employeed);


### *Problem 2. Find three (3) highest earning salary*

SELECT * FROM (
SELECT * FROM employees
ORDER BY salary DESC)
WHERE ROWNUM <= 3

## Multi-Value Sub-Query

### *Problem 3. Select first_name, last_name and department_id of all employees whose location_id =1700
### *Problem 3.1. Select first_name, last_name and department_id of all employees whose location_id !=1700

### 3: Solution
select first_name, last_name, department_id FROM EMPLOYEES WHERE DEPARTMENT_ID in(
(select department_id from departments where location_id =1700));

### 3.1: Solution
select first_name, last_name, department_id FROM EMPLOYEES WHERE DEPARTMENT_ID in(
(select department_id from departments where location_id !=1700));



## Select Top salary from Each Department
SELECT MAX(SALARY) AS MAX_SAL, DEPARTMENT_ID FROM EMPLOYEES GROUP BY DEPARTMENT_ID ORDER BY MAX_SAL DESC;















## Oracle Sandbox Credentials:

Database Information:
Oracle SID    : orclcdb
Pluggable DB  : orcl
Pluggable DB  : ords create if required by running: 
    'newpdbords' in the terminal - for ORDS pdb creation.
         (if 'sqlplus system/oracle@ORDS' connects ORDS pdb has already been installed)
    'loadstorm' in the terminal for spatial demo data - takes a few minutes.

ALL PASSWORDS ARE : oracle
