select
    dept_id,
    is_remote,
    full_name,
    substring(level, 1, 2) as level_prefix
from employees
where email not like '%@outlook.com';
