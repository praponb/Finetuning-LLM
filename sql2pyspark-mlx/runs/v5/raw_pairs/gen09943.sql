select s.*
from departments s
where not exists (
        select 1
        from employees c
        where c.dept_id = s.id
          and c.job_title <> 'Engineer'
    )
