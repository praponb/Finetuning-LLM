with metrics as (
    select
        *,
        row_number() over (partition by dept_name, location order by budget, id) as row_seq,
        percent_rank() over w_hist as budget_percent_rank,
        row_number() over (partition by dept_name order by budget desc, id) as row_seq_2,
        first_value(dept_name, true) over (partition by dept_name order by budget, id rows between unbounded preceding and current row) as first_known_dept_name,
        avg(budget) over (partition by location order by budget, id rows between 5 preceding and current row) as budget_avg_5
    from departments
    window w_hist as (partition by dept_name order by budget desc)
),
latest_match as (
    select
        s.*,
        top1.employee_job_title,
        top1.employee_level
    from metrics s
    join lateral (
            select c.job_title as employee_job_title, c.level as employee_level
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) top1 on true
),
cleaned as (
    select
        *,
        concat(location, ', ', employee_job_title) as location_employee_job_title,
        coalesce(budget_avg_5, 0) as budget_avg_5_filled
    from latest_match
    where budget_avg_5 between 2462326 and 3205503
)
select
    employee_job_title,
    case when budget >= 3569000 then 'PREMIUM' else 'STANDARD' end as budget_tier,
    sort_array(collect_set(employee_level)) as employee_level_list,
    round(avg(budget_avg_5_filled), 2) as avg_budget_avg_5_filled
from cleaned
group by 1, 2
having round(avg(budget_avg_5_filled), 2) < 0
