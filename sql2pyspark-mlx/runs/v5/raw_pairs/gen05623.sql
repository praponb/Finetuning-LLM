with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(location) as clean_location,
        length(dept_name) as dept_name_length
    from departments
    where budget between 2862000 and 2934000
),
metrics as (
    -- Ranking and gap features
    select
        id,
        location,
        budget,
        clean_location,
        dept_name_length,
        sum(dept_name_length) over (partition by location order by budget, id rows between unbounded preceding and current row) as running_dept_name_length,
        row_number() over (order by budget desc, id) as seq_no
    from valid
),
deduped as (
    -- QUALIFY-style window filter
    select id, dept_name_length
    from metrics
    qualify row_number() over (partition by location order by running_dept_name_length desc nulls first, id) = 1
),
metrics_departments as (
    -- Ranking and gap features
    select
        id,
        dept_name_length,
        ntile(4) over w_hist as dept_name_length_bucket,
        lag(dept_name_length, 1) over w_hist as prev_dept_name_length
    from deduped
    where dept_name_length != 55
    window w_hist as (order by dept_name_length, id)
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        dept_name_length,
        prev_dept_name_length,
        lag(dept_name_length_bucket) over w_running as prev_dept_name_length_bucket,
        lead(dept_name_length) over w_running as next_dept_name_length,
        row_number() over w_running as event_seq
    from metrics_departments
    window w_running as (order by dept_name_length_bucket, id)
),
metrics_departments_2 as (
    -- Window metrics over several different partitions
    select
        *,
        avg(next_dept_name_length) over (order by next_dept_name_length, id rows between 2 preceding and 1 preceding) as next_dept_name_length_avg_2_prior,
        row_number() over (order by dept_name_length desc, id) as event_seq_2,
        lag(next_dept_name_length, 1) over (order by event_seq, id) as prev_next_dept_name_length
    from enriched
)
select
    id,
    dept_name_length,
    event_seq
from metrics_departments_2
order by id, dept_name_length;
