with cleaned as (
    select
        *,
        upper(location) as location_upper,
        coalesce(location, 'UNKNOWN') as location_filled
    from departments
    where location is distinct from 'London'
      and dept_name = 'Marketing'
),
with_latest as (
    select
        s.*,
        top1.level,
        top1.hire_date
    from cleaned s
    join lateral (
            select c.level, c.hire_date
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) top1 on true
),
not_flagged as (
    select s.*
    from with_latest s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.level = 'L2'
        )
),
kept as (
    select s.*
    from not_flagged s
    where exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.hire_date < s.hire_date
        )
),
daily as (
    select
        case when budget >= 4152000 then 'LARGE' else 'SMALL' end as budget_tier,
        level,
        count(*) filter (where location = 'London') as london_rows,
        sum(budget) as total_budget,
        percentile_approx(budget, 0.5) as budget_p50,
        round(avg(budget), 2) as avg_budget,
        count(*) filter (where dept_name = 'Finance') as finance_rows,
        stddev_samp(budget) as budget_stddev
    from kept
    group by case when budget >= 4152000 then 'LARGE' else 'SMALL' end, level
)
select
    budget_tier,
    level,
    london_rows,
    total_budget,
    finance_rows,
    budget_stddev
from daily
order by finance_rows;
