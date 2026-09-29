select * from (select location, dept_name, budget from departments) pivot (avg(budget) for dept_name in ('Legal', 'Finance')) order by location
