with with_dims as (
    select
        src.*,
        p.dept_name,
        p.budget
    from employees src
    inner join departments p
        on src.dept_id = p.id
),
with_dims_employees as (
    select
        e.*,
        p.budget as department_budget,
        p.location
    from with_dims e
    left join departments p
        on e.dept_id = p.id
),
metrics as (
    select
        *,
        avg(salary) over (partition by manager_id order by department_budget, id rows between 3 preceding and current row) as salary_avg_3,
        row_number() over (partition by manager_id order by hire_date, id) as event_seq,
        percent_rank() over (partition by manager_id order by department_budget nulls last) as department_budget_percent_rank,
        count(salary) over (partition by dept_name) as dept_name_count_salary
    from with_dims_employees
)
select
    date_trunc('quarter', hire_date) as quarter_start,
    case when department_budget_percent_rank >= 0.68 then 'PREMIUM' else 'STANDARD' end as department_budget_percent_rank_tier,
    size(collect_set(location)) as num_locations,
    count_if(location = 'London') as london_events,
    count(distinct dept_name) as distinct_dept_names,
    approx_percentile(budget, 0.5) as budget_p50,
    round(sum(dept_name_count_salary) / nullif(count(case when location = 'Bangkok' then 1 end), 0), 2) as dept_name_count_salary_per_bangkok,
    rank() over (partition by date_trunc('quarter', hire_date) order by count_if(location = 'London') desc) as london_events_rank
from metrics
group by date_trunc('quarter', hire_date), case when department_budget_percent_rank >= 0.68 then 'PREMIUM' else 'STANDARD' end
order by distinct_dept_names;
