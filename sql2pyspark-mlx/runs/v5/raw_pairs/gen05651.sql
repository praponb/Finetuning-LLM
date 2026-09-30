with lines as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) e as key, attr_value
),
scored as (
    select
        id,
        key,
        attr_value,
        round(status_code / sum(status_code) over (partition by host), 4) as status_code_share,
        sum(status_code) over (order by cast(log_ts as bigint) asc range between 7200 preceding and current row) as status_code_sum_2h,
        cume_dist() over (partition by service, key order by status_code desc) as status_code_cume_dist,
        round(status_code / sum(status_code) over (partition by host, service), 4) as status_code_share_2
    from lines
    where host = 'h2'
),
enriched as (
    select
        attr_value,
        status_code_share,
        status_code_sum_2h,
        percent_rank() over (order by status_code_sum_2h desc nulls first) as status_code_sum_2h_percent_rank,
        count(status_code_share_2) over (partition by key) as key_count_status_code_share_2
    from scored
),
enriched_app_logs as (
    select
        *,
        rank() over w_hist as status_code_sum_2h_rank,
        percent_rank() over (order by status_code_sum_2h asc) as status_code_sum_2h_percent_rank_2,
        rank() over (order by status_code_sum_2h desc nulls first) as status_code_sum_2h_rank_2
    from enriched
    window w_hist as (order by status_code_sum_2h desc nulls last)
),
filtered as (
    select
        attr_value,
        status_code_share,
        status_code_sum_2h,
        status_code_sum_2h_percent_rank,
        key_count_status_code_share_2,
        status_code_sum_2h_percent_rank_2,
        status_code_sum_2h_rank_2,
        coalesce(status_code_sum_2h_rank_2, 0) as status_code_sum_2h_rank_2_filled,
        status_code_sum_2h_rank_2 / nullif(status_code_sum_2h_percent_rank, 0) as status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank
    from enriched_app_logs
    where status_code_sum_2h_rank >= 4
)
select
    case when status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank >= 68 then 'PREMIUM' else 'STANDARD' end as status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank_tier,
    case when status_code_sum_2h_rank_2 >= 7 then 'LARGE' else 'SMALL' end as status_code_sum_2h_rank_2_tier,
    approx_percentile(status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank, array(0.25, 0.5, 0.75)) as status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank_quartiles,
    sum(status_code_sum_2h) as total_status_code_sum_2h,
    round(avg(status_code_sum_2h), 2) as avg_status_code_sum_2h,
    count(*) as num_rows,
    approx_percentile(status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank, array(0.25, 0.5, 0.75)) as status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank_quartiles_2,
    dense_rank() over (partition by case when status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank >= 68 then 'PREMIUM' else 'STANDARD' end order by sum(status_code_sum_2h) desc) as total_status_code_sum_2h_dense_rank
from filtered
group by case when status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank >= 68 then 'PREMIUM' else 'STANDARD' end, case when status_code_sum_2h_rank_2 >= 7 then 'LARGE' else 'SMALL' end
having count(*) >= 1
