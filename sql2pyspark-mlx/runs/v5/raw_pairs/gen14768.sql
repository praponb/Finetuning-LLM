with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        email,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        trim(level) as clean_level,
        coalesce(job_title, 'OTHER') as resolved_job_title
    from employees
    where manager_id is not null
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from filtered
    qualify rank() over (partition by level, job_title order by hire_date desc nulls first) = 1
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        job_title,
        salary,
        is_remote,
        level,
        add_months(hire_date, 3) as hire_date_plus_months,
        coalesce(job_title, 'N/A') as job_title_filled
    from latest
    where job_title is not null
      and hire_date >= '2025-10-01'
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from cleaned
    qualify row_number() over (partition by level, job_title order by salary desc, id) = 1
),
matched_keys as (
    -- Set intersection on keys
    select level, job_title
    from deduped
    intersect all
    select level, job_title
    from employees
    where level = 'L3'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    join matched_keys k on s.level = k.level and s.job_title = k.job_title
),
grouped as (
    -- Aggregate with conditional metrics
    select
        case when salary >= 125000 then 'LARGE' else 'SMALL' end as salary_tier,
        sort_array(collect_set(job_title)) as job_title_list,
        sum(case when level = 'L4' then salary else 0 end) as l4_salary,
        count(*) filter (where job_title = 'Designer') as designer_rows
    from kept_rows
    group by case when salary >= 125000 then 'LARGE' else 'SMALL' end
)
select
    salary_tier,
    job_title_list,
    designer_rows
from grouped
order by salary_tier desc nulls first
