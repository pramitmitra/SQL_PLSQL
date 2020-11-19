# Reading Notes from Paper - “One Size Fits All”: An Idea Whose Time Has Come and Gone by Michael Stonebraker" #
https://cs.brown.edu/~ugur/fits_all.pdf
* Data warehouses are very different from OLTP systems. OLTP systems have been optimized for updates, as the main business activity is typically to sell a good or service,
* In contrast,  the main activity in data warehouses is ad-hoc queries, which are often quite complex. Hence periodic load of new data intersprersed with ad-hoc query activity is what a typical warehouse experience.
* Star Schemas are omni-present in warehouse environments, but are virtually non-existence in OLTP environment.
* ER-Model for OLTP systems - 
* OLTP sytem prefers to use B-Tree indexes.
* DWH system prefers to use Bit-Map indexes.
* Hence, traditional RBDMS vendors provides support for both B-Tree and Bit-Map indexes.
* Materialized views are a useful optimization tactic in warehouse worlds, but never in OLTP worlds. I?n contrast, normal ("virtual") views find acceptance in OLTP environments.

# Spark SQL Performance Tuning Tips #
https://teradatabasics.blogspot.com/?view=classic


# **SQL** #

## Single Value Sub-Query: Sub-query in the where clasuse of a SQL query ##

### Problem 1. Find highest earning salary. ###
```
select * from employee where salary =
(select max(salary) from employeed);
```

### Problem 2. Find three (3) highest earning salary. ###
```
SELECT * FROM (
SELECT * FROM employees
ORDER BY salary DESC)
WHERE ROWNUM <= 3;
```

### Problem 3. Query the list of CITY names starting with vowels (i.e., a, e, i, o, or u) from STATION. Your result cannot contain duplicates. ###
```
SELECT DISTINCT CITY
FROM STATION
WHERE lower(substr(CITY,1,1)) in('a','e','i','o','u');
```

### Problem 4. Query the list of CITY names ending with vowels (a, e, i, o, u) from STATION. Your result cannot contain duplicates. ###
```
SELECT DISTINCT CITY
FROM STATION
WHERE lower(substr(CITY,-1,1)) in('a','e','i','o','u');
```

### Problem 5. Query the list of CITY names from STATION which have vowels (i.e., a, e, i, o, and u) as both their first and last characters. ###
```
SELECT DISTINCT CITY
FROM STATION
WHERE lower(substr(CITY,-1,1)) in('a','e','i','o','u')
AND lower(substr(CITY,1,1)) in('a','e','i','o','u');
```

## * Multi-Value Sub-Query * ###

### *Problem 5. Select first_name, last_name and department_id of all employees whose location_id =1700*
### *Problem 5.1. Select first_name, last_name and department_id of all employees whose location_id !=1700*

### 5: Solution
```
select first_name, last_name, department_id FROM EMPLOYEES WHERE DEPARTMENT_ID in(
(select department_id from departments where location_id =1700));
```

### 5.1: Solution
select first_name, last_name, department_id FROM EMPLOYEES WHERE DEPARTMENT_ID in(
(select department_id from departments where location_id !=1700));

## Correlated Subquery: Inner Query depends on value provided by external query. Inner query is executed multiple times, once per the external query

### *List of employees that receive hight salaries than their respective departmental average.* ###
### Note, inner query to execute multiple time for outher. Its a self join with employees table. ###

SELECT EMPLOYEE_ID, SALARY, DEPARTMENT_ID FROM EMPLOYEES E1 WHERE 
SALARY > (SELECT AVG(SALARY) FROM EMPLOYEES E2
WHERE E1.DEPARTMENT_ID = E2.DEPARTMENT_ID);


## Multi-Column Subquery: returns more than one column
### *Problem 4. Select Employees who earn just the minimum salary for their department.*

select * from employees where (salary, job_id) in(
select min_salary, job_id
from jobs)

## In-Line Query: When a multi-column sub-query is used in a from clause its called in-line view.
### *Problem 5. How Many employees are assigned to each department, including department_id that comes from departments table.*

SELECT * FROM (
SELECT  DEPARTMENT_ID, COUNT(*) EMP_COUNT
FROM EMPLOYEES
GROUP BY DEPARTMENT_ID) emp JOIN DEPARTMENTS deps ON emp.DEPARTMENT_ID= deps.DEPARTMENT_ID;

### *Problem 6. Select top salaries who doesn't earn commisions.
SELECT * FROM EMPLOYEES
WHERE COMMISSION_PCT IS NULL
ORDER BY SALARY DESC

### *Problem 6. Select top three (3) salaries who doesn't earn commisions.###

SELECT * FROM (
SELECT * FROM EMPLOYEES
WHERE COMMISSION_PCT IS NULL
ORDER BY SALARY DESC)top_salaries
WHERE ROWNUM <=3


## Select Top salary from Each Department ##
SELECT MAX(SALARY) AS MAX_SAL, DEPARTMENT_ID FROM EMPLOYEES GROUP BY DEPARTMENT_ID ORDER BY MAX_SAL DESC;




## B-Tree Index: Data Structure similiar to tree. Branch block for search and leaf block to store value.

## PL-SQL (Procedural SQL)
### Since SQL lacks certain programming features, like declaring variable, storing intermediate results or looping through, so oracle has provided feature to write program snippet using PLSQL.

*   Features:
    *   Procedures
    *   Function 
    *   Packages

```javascript

CREATE PROCEDURE hr.update_emp_sal (P_EMP_ID IN NUMBER, SAL_RAISE IN NUMBER)
AS
    V_EMP_CURRENT_SAL_NUMBER;
BEGIN
SELECT SALARY INTO V_EMP_CURRENT_SAL FROM EMPLOYEES WHERE
EMPLOYEE_ID=P_EMP_ID

    UPDATE employees
   SET salary=V_EMP_CURRENT_SAL+SAL_RAISE
    WHERE employee_id=P_EMP_ID;
    
 EXCEPTION WHEN OTHERS THEN
   RAISE_APPLICATION_ERROR (-20001, 'An error was encountered - '||SQLCODE||' ERROR - '|| SQLERRM);
   ROLLBACK;
 COMMIT;
END;
   
```


/*
CoderPad provides a basic SQL sandbox with the following schema.
You can also use commands like `show tables` and `desc employees`

employees                                 projects
+---------------+---------+           +---------------+---------+
| id                      | int     |<----+  +->| id            | int     |
| first_name        | varchar |      |  |  | title         | varchar |
| last_name         | varchar |      |  |  | start_date    | date    |
| salary                | int     |         |  |  | end_date      | date    |
| department_id  | int     |--+     |  |  | budget        | int     |
+---------------+---------+  |  |  |  +---------------+---------+
                             |  |  |
departments                  |  |  |      employees_projects
+---------------+---------+  |  |  |  +---------------+---------+
| id                | int     |<-+    |  +--| project_id    | int     |
| name          | varchar |     +-----| employee_id   | int     |
+---------------+---------+           +---------------+---------+
*/
/*
Q1. Generate a report with all employee details who are drawing highest salary by each department

Q2. Find out employees who has not worked on any project between 2009-01-01 and 2012-12-31 date period.

Q3. Assume you are given a file with list of employee records that needs to be deleted from employees table. How would you achieve this with or without using delete statement?

Q4. Assume you have given a very large pipe delimited data file (text format), and that has different number of fields separated by '|'
How do you cleanse this file by extracting only records with 3 fields and load that data into a different file/table.

Using the same original file assume you have a Customer Review column in position number 3.
Find out the count of all  characters in each Customer review? And display the total number of characters in all of the reviews.

Original file contents sample:

Customer_ID | Customer_Name | Customer_Review
1|c1|I liked the front desk people who cares their customer's needs.
1|c1|I liked the front desk people who cares their customer's needs.|This is a bad record
2|c2|I liked the quality and pricing of their goods.
3|c3|I liked the quality and pricing of their goods.| This is another bad record | sddsfdsfs

Total number of characters in all of the reviews:
63
63+47 = 110
and so on..

*/














## Oracle Sandbox Credentials:

Database Information:
Oracle SID    : orclcdb
Pluggable DB  : orcl
Pluggable DB  : ords create if required by running: 
    'newpdbords' in the terminal - for ORDS pdb creation.
         (if 'sqlplus system/oracle@ORDS' connects ORDS pdb has already been installed)
    'loadstorm' in the terminal for spatial demo data - takes a few minutes.

ALL PASSWORDS ARE : oracle
