with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view explode(context) li as attr_key, value
),
parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        status_code,
        attr_key,
        value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 1) != 0 as int) as is_flagged
    from lines
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        last_value(attr_key, true) over (partition by attr_key order by status_code asc, id asc, attr_key asc rows between unbounded preceding and current row) as last_known_attr_key,
        row_number() over (partition by attr_key order by status_code desc, id asc, attr_key asc) as seq_no
    from parsed
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by attr_key order by status_code_high desc) as dedup_rank
        from enriched
    ) t
    where dedup_rank = 1
),
enriched_app_logs as (
    -- Running and moving aggregates
    select
        *,
        count(status_code_high) over (partition by attr_key) as attr_key_count_status_code_high,
        avg(seq_no) over (partition by attr_key order by status_code_high asc, id asc, attr_key asc rows between 5 preceding and 1 preceding) as seq_no_avg_5_prior,
        first_value(seq_no) ignore nulls over (partition by attr_key order by status_code asc, id asc, attr_key asc rows between unbounded preceding and current row) as first_known_seq_no,
        ntile(10) over (partition by attr_key order by status_code asc, id asc, attr_key asc) as status_code_bucket,
        row_number() over (partition by attr_key order by dedup_rank asc, id asc, attr_key asc) as event_seq
    from ranked
)
-- Ranking and gap features
select
    *,
    sum(status_code_bucket) over (partition by attr_key order by status_code asc, id asc, attr_key asc rows between unbounded preceding and current row) as running_status_code_bucket,
    sum(status_code) over (partition by attr_key order by seq_no_avg_5_prior asc, id asc, attr_key asc rows between unbounded preceding and current row) as running_status_code,
    sum(seq_no_avg_5_prior) over (partition by attr_key order by seq_no asc, id asc, attr_key asc rows between unbounded preceding and current row) as cumulative_seq_no_avg_5_prior
from enriched_app_logs
