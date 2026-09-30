with cleaned as (
    select *, date_add(ends_on, 86) as ends_on_plus_86d
    from promotions
    where ends_on > date '2023-12-10'
),
cleaned_promotions as (
    select
        *,
        date_format(starts_on, 'yyyy-MM-dd') as starts_on_label,
        coalesce(channel, 'N/A') as resolved_channel
    from cleaned
    where ends_on_plus_86d between '2024-03-22' and '2024-12-21'
),
metrics as (
    select
        id,
        channel,
        discount_pct,
        ends_on_plus_86d,
        starts_on_label,
        lead(discount_pct) over w_recent as next_discount_pct,
        lag(discount_pct) over w_recent as prev_discount_pct,
        row_number() over w_recent as seq_no,
        last_value(channel, true) over w_grp as last_known_channel
    from cleaned_promotions
    window w_recent as (partition by channel order by ends_on, id), w_grp as (partition by channel order by starts_on, id rows between unbounded preceding and current row)
),
enriched as (
    select
        id,
        channel,
        ends_on_plus_86d,
        starts_on_label,
        next_discount_pct,
        seq_no,
        last_known_channel,
        ntile(10) over (partition by channel order by discount_pct desc, id) as discount_pct_bucket,
        first_value(prev_discount_pct, true) over (order by discount_pct, id rows between unbounded preceding and current row) as first_known_prev_discount_pct,
        row_number() over w_grp as event_seq
    from metrics
    where next_discount_pct between 0.23 and 0.27
    window w_grp as (partition by channel order by prev_discount_pct, id)
),
scored as (
    select
        *,
        min(next_discount_pct) over w_hist as channel_min_next_discount_pct,
        ntile(4) over w_grp as first_known_prev_discount_pct_bucket
    from enriched
    window w_hist as (partition by channel), w_grp as (partition by channel order by first_known_prev_discount_pct desc, id)
),
enriched_promotions as (
    select
        id,
        channel,
        next_discount_pct,
        seq_no,
        last_known_channel,
        first_known_prev_discount_pct,
        first_known_prev_discount_pct_bucket,
        lag(channel_min_next_discount_pct, 1) over (partition by channel order by first_known_prev_discount_pct, id) as prev_channel_min_next_discount_pct,
        lag(first_known_prev_discount_pct_bucket, 2, 0) over (partition by channel order by first_known_prev_discount_pct_bucket, id) as prev_first_known_prev_discount_pct_bucket,
        lag(discount_pct_bucket, 2) over (partition by channel order by channel_min_next_discount_pct, id) as prev_discount_pct_bucket
    from scored
)
select
    channel,
    sum(case when channel = 'partner' then prev_first_known_prev_discount_pct_bucket else 0 end) as partner_prev_first_known_prev_discount_pct_bucket,
    percentile_approx(prev_discount_pct_bucket, 0.95) as prev_discount_pct_bucket_p95,
    round(sum(prev_channel_min_next_discount_pct) / nullif(count(case when channel = 'store' then 1 end), 0), 2) as prev_channel_min_next_discount_pct_per_store,
    max(next_discount_pct) as max_next_discount_pct
from enriched_promotions
group by channel
order by prev_discount_pct_bucket_p95 desc nulls last
