with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        concat_ws('-', channel, promo_code) as channel_promo_code_key
    from promotions
    where ends_on <= '2025-04-19'
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        channel_promo_code_key
    from (
        select *, row_number() over (partition by promo_code order by ends_on, id) as rank_in_group
        from filtered
    ) t
    where rank_in_group = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        channel_promo_code_key,
        row_number() over w_grp as seq_no,
        row_number() over w_ordered as event_seq,
        row_number() over w_ordered as seq_no_2
    from deduped
    window w_grp as (order by ends_on desc, id), w_ordered as (partition by promo_code order by ends_on desc, id)
),
metrics as (
    -- Ranking and gap features
    select
        *,
        last_value(seq_no_2, true) over (order by seq_no, id rows between unbounded preceding and current row) as last_known_seq_no_2,
        ntile(10) over (order by seq_no_2 desc, id) as seq_no_2_bucket,
        ntile(10) over (order by seq_no, id) as seq_no_bucket,
        sum(event_seq) over (order by seq_no_2, id rows between unbounded preceding and current row) as running_event_seq
    from enriched
    where event_seq between 3 and 5
),
enriched_promotions as (
    -- Window metrics over several different partitions
    select
        *,
        avg(seq_no) over (order by running_event_seq, id rows between 5 preceding and current row) as seq_no_avg_5,
        avg(seq_no_2) over (order by seq_no_2, id rows between 5 preceding and current row) as seq_no_2_avg_5,
        sum(seq_no_bucket) over w_part as cumulative_seq_no_bucket
    from metrics
    window w_part as (order by seq_no_2_bucket, id rows between unbounded preceding and current row)
)
-- Multi-level aggregation
select
    seq_no,
    approx_percentile(seq_no_bucket, 0.95) as seq_no_bucket_p95,
    approx_percentile(seq_no_avg_5, array(0.25, 0.5, 0.75)) as seq_no_avg_5_quartiles,
    count(distinct id) as distinct_ids
from enriched_promotions
group by seq_no
order by seq_no_bucket_p95 desc nulls last;
