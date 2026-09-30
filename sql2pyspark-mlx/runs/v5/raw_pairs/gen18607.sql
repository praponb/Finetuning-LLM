select
    id,
    job_title,
    full_name,
    cast(is_remote as int) as is_remote_flag
from employees
where (job_title in ('Director', 'Designer') and level = 'L2') or dept_id in (14, 27, 29, 35);
