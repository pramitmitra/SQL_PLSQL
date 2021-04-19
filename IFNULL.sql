# Write your MySQL query statement below
select 
e.name,
b.bonus
from Employee e left outer join bonus b
on e.empid=b.empid
where ifnull(bonus,0)<1000