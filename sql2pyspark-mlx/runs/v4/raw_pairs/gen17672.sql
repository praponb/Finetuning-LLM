select
    manager_id,
    job_title,
    level,
    upper(email) as email_upper,
    lower(level) as level_lower
from employees
where salary < 124000;
