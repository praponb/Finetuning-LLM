with metrics as (
    -- Ranking and gap features
    select
        id,
        full_name,
        email,
        manager_id,
        hire_date,
        is_remote,
        rank() over (partition by manager_id order by salary desc) as salary_rank,
        avg(salary) over (partition by level order by hire_date, id rows between 2 preceding and current row) as salary_avg_2,
        dense_rank() over (partition by dept_id order by salary desc) as salary_dense_rank
    from employees
),
kept_keys as (
    -- Set difference on keys
    select manager_id
    from metrics
    except distinct
    select manager_id
    from employees
    where manager_id in (15, 18, 22, 27)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    join kept_keys k on s.manager_id = k.manager_id
),
filtered as (
    -- Basic cleansing and defaults
    select *, case when is_remote then 'Yes' else 'No' end as is_remote_label
    from reconciled
    where hire_date > to_date('2023-08-19')
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        manager_id,
        hire_date,
        salary_rank,
        salary_avg_2,
        salary_dense_rank,
        round(salary_avg_2 / sum(salary_avg_2) over (partition by manager_id), 4) as salary_avg_2_share
    from filtered
),
scored_employees as (
    -- Ranking and gap features
    select
        *,
        round(salary_avg_2 / sum(salary_avg_2) over (partition by manager_id), 4) as salary_avg_2_share_2,
        count(salary_dense_rank) over (partition by manager_id) as manager_id_count_salary_dense_rank,
        rank() over (partition by manager_id order by salary_avg_2 desc) as salary_avg_2_rank
    from scored
    where salary_dense_rank < 6
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        full_name,
        hire_date,
        salary_rank,
        salary_avg_2,
        salary_dense_rank,
        salary_avg_2_share_2,
        manager_id_count_salary_dense_rank,
        salary_avg_2_rank,
        last_day(hire_date) as hire_date_month_end,
        salary_rank + salary_avg_2_share_2 as salary_rank_plus_salary_avg_2_share_2
    from scored_employees
    where salary_dense_rank > 5
),
scored_employees_2 as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        hire_date,
        salary_avg_2,
        salary_dense_rank,
        salary_avg_2_rank,
        dense_rank() over w_grp as salary_rank_plus_salary_avg_2_share_2_dense_rank,
        rank() over w_recent as salary_avg_2_rank_2
    from cleaned
    window w_grp as (order by salary_rank_plus_salary_avg_2_share_2 nulls first), w_recent as (order by salary_avg_2 desc)
)
-- Grouped metrics
select
    date_trunc('month', hire_date) as activity_month,
    case when salary_dense_rank >= 8 then 'HIGH' else 'LOW' end as salary_dense_rank_tier,
    approx_percentile(salary_rank_plus_salary_avg_2_share_2_dense_rank, array(0.25, 0.5, 0.75)) as salary_rank_plus_salary_avg_2_share_2_dense_rank_quartiles,
    count(distinct id) as distinct_ids,
    min(salary_avg_2_rank) as min_salary_avg_2_rank,
    rank() over (partition by date_trunc('month', hire_date) order by count(distinct id) desc) as distinct_ids_rank
from scored_employees_2
group by 1, 2
having distinct_ids > 1
