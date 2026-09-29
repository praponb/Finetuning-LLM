select * from (select dept_name, location, budget from departments) pivot (sum(budget) for location in ('Bangkok', 'Berlin')) order by dept_name;
