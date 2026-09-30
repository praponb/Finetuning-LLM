select *, lower(level) as level_lower, concat(email, ', ', full_name) as email_full_name from employees where email is not null order by dept_id
