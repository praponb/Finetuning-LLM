with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when dept_name = 'Legal' then 1 else 0 end as is_legal,
        coalesce(dept_name, 'OTHER') as resolved_dept_name
    from departments
    where budget <= 2619000
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        dept_name,
        location,
        budget,
        is_legal,
        resolved_dept_name,
        sum(is_legal) over (partition by dept_name order by budget, id rows between unbounded preceding and current row) as running_is_legal,
        percent_rank() over (partition by dept_name order by is_legal nulls first) as is_legal_percent_rank
    from base
),
with_last as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.job_title,
        x.employee_level
    from with_windows s
    join lateral (
            select c.job_title, c.level as employee_level
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) x on true
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_last s
    where exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.salary <= 63000
        )
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by dept_name, job_title order by is_legal nulls first, id) as latest_rank
    from eligible
    qualify latest_rank = 1
)
-- Aggregate with conditional metrics
select
    dept_name,
    employee_level,
    round(sum(is_legal_percent_rank) / nullif(count(case when employee_level = 'L2' then 1 end), 0), 2) as is_legal_percent_rank_per_l2,
    count(*) filter (where location = 'Tokyo') as tokyo_rows,
    count(*) filter (where employee_level = 'L4') as l4_rows,
    count(case when dept_name = 'Legal' then 1 end) as legal_count,
    percentile_approx(is_legal_percent_rank, 0.9) as is_legal_percent_rank_p90,
    rank() over (partition by dept_name order by count(*) filter (where location = 'Tokyo') desc) as tokyo_rows_rank
from latest
group by dept_name, employee_level
having percentile_approx(is_legal_percent_rank, 0.9) < 0
order by is_legal_percent_rank_p90 desc nulls last
