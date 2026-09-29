with filtered as (
    -- Keep recent, valid rows
    select
        id,
        full_name,
        dept_id,
        job_title,
        salary,
        hire_date,
        level,
        regexp_extract(email, '@(.+)$', 1) as email_domain,
        coalesce(level, 'UNKNOWN') as level_filled
    from employees
    where manager_id is not null
),
scored as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        salary,
        hire_date,
        level,
        sum(salary) over w_part as cumulative_salary,
        row_number() over (partition by dept_id, level order by salary desc, id asc) as event_seq
    from filtered
    window w_part as (partition by job_title, level order by salary asc, id asc rows between unbounded preceding and current row)
),
latest as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by level order by hire_date asc nulls first) as rank_in_group
    from scored
    qualify rank_in_group = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        lead(event_seq) over w_running as next_event_seq,
        lag(cumulative_salary) over w_running as prev_cumulative_salary,
        row_number() over w_running as row_seq,
        round(event_seq / sum(event_seq) over (partition by level), 4) as event_seq_share
    from latest
    where cumulative_salary between 222597 and 704821
    window w_running as (partition by level order by salary asc, id asc)
),
enriched_employees as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        salary,
        hire_date,
        level,
        event_seq,
        rank_in_group,
        lead(row_seq) over w_part as next_row_seq,
        lag(row_seq) over w_part as prev_row_seq,
        lag(event_seq) over w_part as prev_event_seq,
        row_number() over w_part as seq_no,
        row_number() over (partition by level order by salary asc, id asc) as seq_no_2
    from enriched
    window w_part as (partition by level order by cumulative_salary asc, id asc)
),
matched_keys as (
    -- Set difference on keys
    select level, id
    from enriched_employees
    except distinct
    select level, id
    from employees
    where level <> 'L4'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched_employees s
    inner join matched_keys k on s.level = k.level and s.id = k.id
),
metrics as (
    -- Window metrics over several different partitions
    select *, round(salary / sum(salary) over (partition by level), 4) as salary_share
    from kept_rows
),
grouped as (
    -- Grouped metrics
    select
        seq_no,
        case when prev_event_seq >= 8 then 'HIGH' else 'LOW' end as prev_event_seq_tier,
        count_if(level = 'L5') as l5_events,
        count(*) as event_count,
        count(case when level = 'L5' then 1 end) as l5_count,
        sort_array(collect_set(level)) as level_list,
        sum(case when level = 'L3' then salary_share else 0 end) as l3_salary_share,
        round(avg(salary), 2) as avg_salary
    from metrics
    group by grouping sets ((seq_no, case when prev_event_seq >= 8 then 'HIGH' else 'LOW' end), (seq_no))
)
select
    seq_no,
    prev_event_seq_tier,
    l5_events,
    event_count,
    level_list,
    l3_salary_share,
    avg_salary
from grouped
order by seq_no asc nulls last;
