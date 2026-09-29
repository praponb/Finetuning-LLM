select *, initcap(trim(level)) as level_title, concat(level, ' ', job_title) as level_job_title from employees where salary > 80000 and is_remote
