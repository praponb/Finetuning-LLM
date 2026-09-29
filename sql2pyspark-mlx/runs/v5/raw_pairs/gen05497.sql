select id, job_title, length(full_name) full_name_length, concat(job_title, ', ', level) job_title_level from employees where email like '%@gmail.com' and level <> 'L2' order by job_title_level desc
