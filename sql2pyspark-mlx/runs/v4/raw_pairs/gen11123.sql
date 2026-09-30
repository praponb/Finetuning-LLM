with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) li as key, attr_value
),
unpacked as (
    -- Semi-structured parsing
    select
        service,
        log_ts,
        latency_ms,
        attr_value,
        cast((status_code & 2) != 0 as int) as flag_bit2,
        shiftright(status_code, 2) as status_code_high
    from exploded
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        avg(flag_bit2) over w_ordered as flag_bit2_avg_30m,
        count(latency_ms) over w_grp as latency_ms_count_1h,
        dense_rank() over (partition by service order by flag_bit2 desc) as flag_bit2_dense_rank
    from unpacked
    window w_ordered as (partition by service order by cast(log_ts as bigint) range between 1800 preceding and current row), w_grp as (order by cast(log_ts as bigint) range between 3600 preceding and current row)
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        round(latency_ms_count_1h / sum(latency_ms_count_1h) over (partition by service), 4) as latency_ms_count_1h_share,
        round(flag_bit2_dense_rank / sum(flag_bit2_dense_rank) over (partition by service), 4) as flag_bit2_dense_rank_share
    from metrics
)
-- Ranking and gap features
select
    *,
    round(latency_ms_count_1h / sum(latency_ms_count_1h) over (partition by service), 4) as latency_ms_count_1h_share_2,
    sum(flag_bit2_avg_30m) over w_hist as flag_bit2_avg_30m_sum_15m
from enriched
window w_hist as (order by cast(log_ts as bigint) range between 900 preceding and current row)
