with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        salary,
        hire_date,
        level,
        quarter(hire_date) as hire_date_quarter
    from employees
    where hire_date > date '2024-01-26'
      and level is not null
      and job_title in ('Engineer', 'Analyst')
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        salary,
        hire_date,
        level,
        hire_date_quarter,
        date_add(hire_date, 42) as hire_date_plus_42d,
        coalesce(level, 'UNKNOWN') as level_filled
    from filtered
    where hire_date >= date '2023-07-26'
      and level is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by level order by hire_date, id) as seq_no,
        cume_dist() over w_recent as hire_date_quarter_cume_dist,
        ntile(5) over w_grp as hire_date_quarter_bucket
    from cleaned
    window w_recent as (partition by level order by hire_date_quarter desc nulls first), w_grp as (order by hire_date_quarter desc, id)
)
-- Aggregate with conditional metrics
select
    seq_no,
    date_trunc('quarter', hire_date) as quarter_start,
    sort_array(collect_set(level)) as level_list,
    count(distinct id) as distinct_ids,
    round(sum(hire_date_quarter_cume_dist) / nullif(count(case when level = 'L4' then 1 end), 0), 2) as hire_date_quarter_cume_dist_per_l4
from enriched
group by grouping sets ((seq_no, date_trunc('quarter', hire_date)), (seq_no), ());
