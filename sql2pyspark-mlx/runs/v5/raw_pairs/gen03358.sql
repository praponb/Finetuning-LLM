select full_name, job_title, email, lower(full_name) as full_name_lower from employees where email like '%@gmail.com'
