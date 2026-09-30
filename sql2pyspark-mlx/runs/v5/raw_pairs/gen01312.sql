with valid as (
    -- Basic cleansing and defaults
    select *, concat(dept_name, '/', location) as dept_name_location
    from departments
    where budget between 1681000 and 3120000
      and location is not null
),
latest_match as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.hire_date,
        top1.employee_level
    from valid s
    join lateral (
            select c.hire_date, c.level as employee_level
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) top1 on true
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from latest_match s
    where exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.hire_date < s.hire_date
        )
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        budget,
        dept_name_location,
        hire_date,
        dense_rank() over (partition by employee_level order by budget desc nulls first) as budget_dense_rank,
        sum(budget) over (partition by location order by budget, id rows between unbounded preceding and current row) as cumulative_budget
    from kept
    where dept_name not in ('Sales', 'HR', 'Legal')
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.hire_date < s.hire_date
        )
),
with_last as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.employee_job_title,
        top1.level
    from not_flagged s
    join lateral (
            select c.job_title as employee_job_title, c.level
            from employees c
            where c.dept_id = s.id
              and c.hire_date <= s.hire_date
            order by c.hire_date desc, c.id desc
            limit 1
        ) top1 on true
),
pivoted as (
    -- Native SQL PIVOT
    select *
    from (
        select
            employee_job_title,
            level,
            budget_dense_rank
        from with_last
    ) src
    pivot (
        sum(budget_dense_rank) as amount,
        count(1) as n
        for level in ('L2', 'L4', 'L1')
    )
)
select
    employee_job_title,
    L4_n,
    L1_amount,
    L1_n,
    case when employee_job_title rlike '^(Manager|Director)$' then 'PRIORITY' when employee_job_title rlike '^(Engineer|Analyst)$' then 'GROUP_B' else 'GROUP_A' end as employee_job_title_zone
from pivoted
order by employee_job_title desc nulls first, L4_n
