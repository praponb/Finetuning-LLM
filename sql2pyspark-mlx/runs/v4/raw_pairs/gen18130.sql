with filtered as (
    select
        id,
        full_name,
        email,
        salary,
        is_remote,
        level,
        concat(level, ' ', full_name) as level_full_name,
        coalesce(level, 'N/A') as resolved_level
    from employees
    where hire_date > date '2023-11-28'
      and level is not null
      and hire_date < '2023-03-01'
),
cleaned as (
    select
        id,
        full_name,
        email,
        salary,
        is_remote,
        level,
        level_full_name,
        resolved_level,
        initcap(trim(level)) as level_title,
        ceil(salary) as salary_whole
    from filtered
    where email like '%@GMAIL.COM'
),
latest as (
    select *, rank() over (partition by level order by salary_whole desc) as rn
    from cleaned
    qualify rn = 1
),
kept_keys as (
    select level, id
    from latest
    except
    select level, id
    from employees
    where not is_remote
),
verified as (
    select s.*
    from latest s
    join kept_keys k on s.level = k.level and s.id = k.id
),
scored as (
    select
        *,
        avg(salary_whole) over w_grp as level_avg_salary_whole,
        count(salary_whole) over w_grp as level_count_salary_whole,
        sum(salary_whole) over w_grp as level_sum_salary_whole
    from verified
    window w_grp as (partition by level)
),
scored_employees as (
    select
        *,
        max(salary_whole) over (partition by level) as level_max_salary_whole,
        rank() over (partition by level order by level_avg_salary_whole nulls first) as level_avg_salary_whole_rank,
        stddev_samp(level_sum_salary_whole) over (partition by level) as level_stddev_samp_level_sum_salary_whole,
        dense_rank() over (order by level_sum_salary_whole desc nulls first) as level_sum_salary_whole_dense_rank
    from scored
),
scored_employees_2 as (
    select
        *,
        cume_dist() over (order by level_avg_salary_whole_rank desc) as level_avg_salary_whole_rank_cume_dist,
        dense_rank() over w_part as salary_whole_dense_rank,
        sum(level_count_salary_whole) over w_recent as level_sum_level_count_salary_whole
    from scored_employees
    window w_part as (order by salary_whole desc nulls last), w_recent as (partition by level)
),
totals as (
    select
        level,
        case when level_count_salary_whole >= 17 then 'PREMIUM' else 'STANDARD' end as level_count_salary_whole_tier,
        count_if(level = 'L3') as l3_events,
        round(sum(salary_whole) / nullif(count(case when level = 'L5' then 1 end), 0), 2) as salary_whole_per_l5,
        sum(case when level = 'L2' then level_sum_salary_whole else 0 end) as l2_level_sum_salary_whole
    from scored_employees_2
    group by level, case when level_count_salary_whole >= 17 then 'PREMIUM' else 'STANDARD' end
    having l3_events < 1
)
select
    level,
    level_count_salary_whole_tier,
    l3_events,
    salary_whole_per_l5,
    l2_level_sum_salary_whole
from totals
order by level_count_salary_whole_tier, salary_whole_per_l5 desc;
