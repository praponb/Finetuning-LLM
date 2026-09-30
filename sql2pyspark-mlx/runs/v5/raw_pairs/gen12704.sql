with lines as (
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view explode(context) x as k, attr_value
),
parsed as (
    select
        id,
        k,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 2) != 0 as int) as flag_bit2
    from lines
),
scored as (
    select
        *,
        avg(flag_bit2) over (partition by k order by flag_bit2, id, k rows between 3 preceding and 1 preceding) as flag_bit2_avg_3_prior,
        ntile(3) over (partition by k order by status_code_high, id, k) as status_code_high_bucket,
        lag(status_code_high, 1) over (partition by k order by flag_bit2, id, k) as prev_status_code_high,
        avg(status_code_high) over (partition by k order by flag_bit2, id, k rows between 5 preceding and current row) as status_code_high_avg_5,
        sum(status_code_high) over (partition by k order by status_code_high, id, k rows between unbounded preceding and current row) as running_status_code_high
    from parsed
    where flag_bit2 = 0
),
deduped as (
    select
        id,
        k,
        flag_bit2_avg_3_prior,
        status_code_high_bucket
    from scored
    qualify row_number() over (partition by k order by status_code_high desc nulls last, id, k) = 1
)
select
    case when flag_bit2_avg_3_prior >= 1 then 'LARGE' else 'SMALL' end as flag_bit2_avg_3_prior_tier,
    count(*) filter (where k = 'trace') as trace_rows,
    count(*) as num_rows,
    sum(status_code_high_bucket) filter (where k = 'version') as version_total_status_code_high_bucket,
    round(sum(status_code_high_bucket) / nullif(count(case when k = 'trace' then 1 end), 0), 2) as status_code_high_bucket_per_trace
from deduped
group by case when flag_bit2_avg_3_prior >= 1 then 'LARGE' else 'SMALL' end;
