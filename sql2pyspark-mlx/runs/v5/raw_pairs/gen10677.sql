with latest_match as (
    select
        s.*,
        top1.employee_hire_date,
        top1.job_title
    from departments s
    left join lateral (
            select c.hire_date as employee_hire_date, c.job_title
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) top1 on true
),
with_windows as (
    select
        *,
        avg(budget) over (partition by location, job_title order by budget, id rows between 6 preceding and current row) budget_avg_6,
        round(budget / sum(budget) over (partition by dept_name), 4) budget_share
    from latest_match
),
with_windows_departments as (
    select
        id,
        dept_name,
        location,
        budget_share,
        avg(budget_avg_6) over (partition by dept_name) dept_name_avg_budget_avg_6,
        dense_rank() over (order by budget_avg_6 nulls first) budget_avg_6_dense_rank,
        row_number() over (partition by job_title order by budget desc, id) seq_no
    from with_windows
    where budget_share >= 0.62
),
with_last as (
    select
        s.id,
        s.dept_name_avg_budget_avg_6,
        s.budget_avg_6_dense_rank,
        top1.employee_job_title
    from with_windows_departments s
    join lateral (
            select c.job_title as employee_job_title
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) top1 on true
),
eligible as (
    select s.*
    from with_last s
    where exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.dept_id in (1, 18)
        )
),
with_latest as (
    select s.*, top1.hire_date
    from eligible s
    left join lateral (
            select c.hire_date
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) top1 on true
)
select
    employee_job_title,
    size(collect_set(employee_job_title)) as num_employee_job_titles,
    count(*) filter (where employee_job_title = 'Designer') as designer_rows,
    approx_percentile(budget_avg_6_dense_rank, array(0.25, 0.5, 0.75)) as budget_avg_6_dense_rank_quartiles
from with_latest
group by employee_job_title;
