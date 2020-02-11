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

### *Problem 3. Select first_name, last_name and department_id of all employees whose location_id =1700*
### *Problem 3.1. Select first_name, last_name and department_id of all employees whose location_id !=1700*

### 3: Solution
select first_name, last_name, department_id FROM EMPLOYEES WHERE DEPARTMENT_ID in(
(select department_id from departments where location_id =1700));

### 3.1: Solution
select first_name, last_name, department_id FROM EMPLOYEES WHERE DEPARTMENT_ID in(
(select department_id from departments where location_id !=1700));

## Correlated Subquery: Inner Query depends on value provided by external query. Inner query is executed multiple times, once per the external query

### *List of employees that receive hight salaries than their respective departmental average.*
### Note, inner query to execute multiple time for outher. Its a self join with employees table.

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

### *Problem 6. Select top three (3) salaries who doesn't earn commisions.
SELECT * FROM (
SELECT * FROM EMPLOYEES
WHERE COMMISSION_PCT IS NULL
ORDER BY SALARY DESC)top_salaries
WHERE ROWNUM <=3


## Select Top salary from Each Department
SELECT MAX(SALARY) AS MAX_SAL, DEPARTMENT_ID FROM EMPLOYEES GROUP BY DEPARTMENT_ID ORDER BY MAX_SAL DESC;




## B-Tree Index: Data Structure similiar to tree. Branch block for search and leaf block to store value.

## PL-SQL (Procedural SQL)
### Since SQL lacks certain programming features, like declaring variable, storing intermediate results or looping through, so oracle has provided feature to write program snippet using PLSQL.

* Features:
** Procedures
** Function 
** Packages

** Procedure:
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
   















## Oracle Sandbox Credentials:

Database Information:
Oracle SID    : orclcdb
Pluggable DB  : orcl
Pluggable DB  : ords create if required by running: 
    'newpdbords' in the terminal - for ORDS pdb creation.
         (if 'sqlplus system/oracle@ORDS' connects ORDS pdb has already been installed)
    'loadstorm' in the terminal for spatial demo data - takes a few minutes.

ALL PASSWORDS ARE : oracle
