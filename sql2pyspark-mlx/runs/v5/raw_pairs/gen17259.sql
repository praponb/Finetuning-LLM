select id, manager_id, trim(level) as clean_level from employees where email like '%@startup.io' and dept_id in (7, 15, 30) order by manager_id desc, id desc;
