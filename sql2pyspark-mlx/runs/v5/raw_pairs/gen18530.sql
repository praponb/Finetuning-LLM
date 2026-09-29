with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        full_name,
        email,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        case when salary >= 155000 then 'High' when salary >= 86000 then 'Medium' else 'Low' end as salary_tier
    from employees
    where job_title = 'Analyst'
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by job_title order by hire_date desc, id) as event_seq,
        last_value(job_title) ignore nulls over (order by hire_date, id rows between unbounded preceding and current row) as last_known_job_title,
        row_number() over (order by hire_date, id) as seq_no
    from base
    where email like '%@company.co'
),
enriched_employees as (
    -- Attach employees attributes
    select
        s.*,
        d.hire_date as employee_hire_date,
        d.full_name as employee_full_name,
        d.job_title as employee_job_title
    from enriched s
    left join employees d
        on s.manager_id = d.id
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, rank() over (partition by job_title order by employee_hire_date desc) as rank_in_group
        from enriched_employees
    ) ranked
    where rank_in_group = 1
),
enriched_employees_2 as (
    -- Attach employees attributes
    select
        s.*,
        e.level,
        e.salary,
        e.job_title as employee_job_title_2
    from deduped s
    left join employees e
        on s.manager_id = e.id
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        cast(is_remote as int) as is_remote_flag,
        case when job_title = 'Director' then 1 else 0 end as is_director
    from enriched_employees_2
    where hire_date > to_date('2023-07-15')
      and level = 'L5'
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        full_name,
        email,
        manager_id,
        is_remote,
        salary_tier,
        last_known_job_title,
        seq_no,
        employee_job_title,
        rank_in_group,
        level,
        salary,
        is_remote_flag,
        is_director,
        ceil(is_director) as is_director_whole
    from cleaned
    where hire_date >= date '2023-04-01'
      and seq_no >= 8
)
select
    id,
    full_name,
    manager_id,
    is_remote_flag,
    is_director,
    is_director_whole,
    case when full_name rlike '^(Emma Brown|Grace Kim)$' then 'GROUP_B' else 'GROUP_A' end as full_name_zone
from filtered;
