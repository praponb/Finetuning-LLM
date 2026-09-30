select *, upper(email) as email_upper, cast(salary as int) as salary_int
from employees
where is_remote and salary > 90000 and email is not null
