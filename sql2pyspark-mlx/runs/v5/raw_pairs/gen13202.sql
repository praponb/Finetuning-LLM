select id, email, case when job_title = 'Designer' then 1 else 0 end as is_designer from employees where salary < 85000 and (full_name is not null or email is not null) order by 3 desc;
