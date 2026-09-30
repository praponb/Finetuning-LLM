with valid as (
    select *, round(budget * 1.1, 2) as budget_scaled
    from departments
    where dept_name != 'Marketing'
),
latest_match as (
    select
        s.*,
        latest.employee_level,
        latest.job_title
    from valid s
    join lateral (
            select c.level as employee_level, c.job_title
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) latest on true
),
latest as (
    select *
    from (
        select *, row_number() over (partition by employee_level order by budget desc, id) as row_num
        from latest_match
    ) t
    where row_num = 1
),
metrics as (
    select
        *,
        min(budget) over w_recent as last3_min_budget,
        count(budget) over w_recent as last3_count_budget,
        sum(budget) over w_recent as last3_sum_budget,
        max(budget) over w_recent as last3_max_budget,
        row_number() over (partition by employee_level order by row_num desc, id) as seq_no
    from latest
    window w_recent as (partition by job_title order by row_num, id rows between 2 preceding and current row)
),
ranked as (
    select *
    from (
        select *, rank() over (partition by job_title order by last3_count_budget) as row_num_2
        from metrics
    ) t
    where row_num_2 = 1
),
with_latest as (
    select
        s.id,
        s.dept_name,
        s.location,
        s.budget,
        s.budget_scaled,
        s.employee_level,
        s.job_title,
        s.row_num,
        s.last3_min_budget,
        top1.salary
    from ranked s
    left join lateral (
            select c.salary
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) top1 on true
),
kept as (
    select s.*
    from with_latest s
    where exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.manager_id in (11, 30, 39)
        )
)
select
    location,
    sum(case when location = 'Singapore' then budget else 0 end) as singapore_budget,
    approx_percentile(budget_scaled, 0.9) as budget_scaled_p90,
    count(*) as cnt,
    round(avg(last3_min_budget), 2) as avg_last3_min_budget,
    stddev_samp(budget) as budget_stddev,
    count(*) filter (where dept_name = 'Marketing') as marketing_rows
from kept
group by location;
