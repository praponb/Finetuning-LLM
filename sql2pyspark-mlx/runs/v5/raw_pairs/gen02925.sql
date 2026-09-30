with with_windows as (
    select
        id,
        email,
        manager_id,
        salary,
        max(salary) over w_running last7_max_salary,
        sum(salary) over w_running last7_sum_salary,
        avg(salary) over w_running last7_avg_salary,
        min(salary) over w_running last7_min_salary
    from employees
    window w_running as (partition by manager_id order by hire_date asc, id asc rows between 6 preceding and current row)
)
select
    id,
    email,
    manager_id,
    last7_max_salary,
    last7_sum_salary,
    last7_min_salary
from with_windows
