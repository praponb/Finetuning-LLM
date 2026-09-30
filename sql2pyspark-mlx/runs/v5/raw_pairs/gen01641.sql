with latest as (
    select
        id,
        manager_id,
        salary,
        is_remote
    from employees
    qualify row_number() over (partition by dept_id order by salary desc, id) = 1
),
cleaned as (
    select *, round(salary * 1.1, 2) as salary_scaled
    from latest
    where is_remote
),
with_windows as (
    select
        id,
        salary,
        salary_scaled,
        avg(salary) over (order by salary, id rows between 5 preceding and current row) as salary_avg_5,
        lag(salary_scaled, 1) over w_recent as prev_salary_scaled,
        min(salary_scaled) over (partition by manager_id) as manager_id_min_salary_scaled
    from cleaned
    window w_recent as (partition by manager_id order by salary_scaled, id)
),
base as (
    select
        id,
        salary,
        salary_avg_5,
        prev_salary_scaled,
        manager_id_min_salary_scaled,
        case when salary_scaled >= 62 then 'High' when salary_scaled >= 11 then 'Medium' else 'Low' end as salary_scaled_level
    from with_windows
    where manager_id_min_salary_scaled = 23
),
valid as (
    select *, manager_id_min_salary_scaled + prev_salary_scaled as manager_id_min_salary_scaled_plus_prev_salary_scaled
    from base
    where salary > 95000
),
scored as (
    select
        id,
        salary_avg_5,
        prev_salary_scaled,
        manager_id_min_salary_scaled,
        row_number() over (order by prev_salary_scaled, id) as event_seq,
        cume_dist() over (order by salary_avg_5 nulls first) as salary_avg_5_cume_dist,
        row_number() over (order by salary desc, id) as row_seq
    from valid
    where salary_avg_5 <> 89226
)
select
    id,
    salary_avg_5,
    salary_avg_5_cume_dist,
    sum(event_seq) over (order by salary_avg_5, id rows between unbounded preceding and current row) as running_sum_event_seq,
    max(event_seq) over (order by salary_avg_5, id rows between unbounded preceding and current row) as running_max_event_seq,
    count(event_seq) over (order by salary_avg_5, id rows between unbounded preceding and current row) as running_count_event_seq,
    dense_rank() over (order by manager_id_min_salary_scaled desc) as manager_id_min_salary_scaled_dense_rank
from scored;
