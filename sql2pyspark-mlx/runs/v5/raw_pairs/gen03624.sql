with metrics as (
    select
        *,
        lead(sqft) over w_ordered as next_sqft,
        row_number() over w_ordered as row_seq,
        round(sqft / sum(sqft) over (partition by city), 4) as sqft_share
    from stores
    window w_ordered as (partition by region, city order by sqft, id)
),
enriched as (
    select
        *,
        ntile(3) over (partition by region order by row_seq desc, id) as row_seq_bucket,
        row_number() over (partition by city order by sqft_share, id) as event_seq,
        sum(row_seq) over w_grp as running_row_seq,
        lead(next_sqft, 1) over (partition by city order by next_sqft, id) as next_next_sqft
    from metrics
    window w_grp as (partition by region order by row_seq, id rows between unbounded preceding and current row)
),
enriched_stores as (
    select
        *,
        sum(event_seq) over (order by opened_date, id rows between unbounded preceding and current row) as running_event_seq,
        avg(next_next_sqft) over (partition by region order by next_next_sqft, id rows between 2 preceding and 1 preceding) as next_next_sqft_avg_2_prior,
        avg(sqft) over (partition by region order by sqft, id rows between 6 preceding and current row) as sqft_avg_6
    from enriched
),
with_latest as (
    select s.*, x.shipping_fee
    from enriched_stores s
    join lateral (
            select c.shipping_fee
            from orders c
            where c.store_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    city,
    metric_name,
    reading
from (
    select
        id,
        city,
        sqft_share,
        shipping_fee
    from with_latest
) src
unpivot (
    reading for metric_name in (sqft_share, shipping_fee)
)
