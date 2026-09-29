select id, dept_id, initcap(trim(job_title)) as job_title_title from employees where hire_date between '2024-06-01' and '2024-07-01' order by dept_id desc, id desc;
