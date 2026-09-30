with scored as (
    select
        id,
        full_name,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        avg(salary) over w_hist as running_avg_salary,
        min(salary) over w_hist as running_min_salary,
        sum(salary) over w_hist as running_sum_salary
    from employees
    window w_hist as (partition by dept_id order by salary, id rows between unbounded preceding and current row)
),
valid as (
    select *, least(running_avg_salary, running_min_salary) as least_running_avg_salary_running_min_salary
    from scored
    where manager_id is not null
      and manager_id in (13, 21)
),
enriched as (
    select
        *,
        row_number() over (order by running_min_salary desc, id) as row_seq,
        row_number() over (partition by manager_id order by running_min_salary desc, id) as row_seq_2
    from valid
    where running_sum_salary > 378000
),
enriched_employees as (
    select
        s.id,
        s.dept_id,
        s.manager_id,
        s.salary,
        s.hire_date,
        s.is_remote,
        b.dept_name,
        b.location,
        b.budget
    from enriched s
    left join departments b
        on s.dept_id = b.id
),
joined as (
    select
        a.*,
        b.location as department_location,
        b.dept_name as department_dept_name,
        b.budget as department_budget
    from enriched_employees a
    left join departments b
        on a.dept_id = b.id
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by location, manager_id order by hire_date desc, id) as row_num
        from joined
    ) t
    where row_num = 1
),
kept_keys as (
    select manager_id, dept_id
    from deduped
    intersect all
    select manager_id, dept_id
    from employees
    where manager_id is not null
),
reconciled as (
    select s.*
    from deduped s
    inner join kept_keys k on s.manager_id = k.manager_id and s.dept_id = k.dept_id
)
select
    manager_id,
    sort_array(collect_set(dept_name)) as dept_name_list,
    approx_percentile(department_budget, array(0.25, 0.5, 0.75)) as department_budget_quartiles
from reconciled
group by manager_id
order by manager_id desc nulls last;
