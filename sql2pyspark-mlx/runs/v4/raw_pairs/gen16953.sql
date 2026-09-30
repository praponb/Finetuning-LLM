with base as (
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        amount * discount amount_times_discount,
        coalesce(channel, 'UNKNOWN') channel_clean
    from orders
    where order_date > date '2024-05-05'
      and channel is not null
),
scored as (
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        amount_times_discount,
        channel_clean,
        avg(amount_times_discount) over (partition by channel order by shipping_fee, id rows between 2 preceding and 1 preceding) amount_times_discount_avg_2_prior,
        avg(amount) over (partition by customer_id order by order_date, id rows between 6 preceding and current row) amount_avg_6
    from base
),
scored_orders as (
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        amount_times_discount,
        channel_clean,
        amount_times_discount_avg_2_prior,
        amount_avg_6,
        avg(amount_avg_6) over (partition by status order by amount, id rows between 6 preceding and current row) amount_avg_6_avg_6,
        last_value(amount_times_discount) ignore nulls over (order by amount_avg_6, id rows between unbounded preceding and current row) last_known_amount_times_discount,
        ntile(10) over (order by amount_avg_6 desc, id) amount_avg_6_bucket,
        lead(amount_times_discount, 1) over (partition by channel order by shipping_fee, id) next_amount_times_discount
    from scored
),
enriched as (
    select
        *,
        row_number() over (partition by customer_id order by amount_avg_6_bucket, id) event_seq,
        first_value(amount_avg_6_bucket) ignore nulls over (partition by status order by order_date, id rows between unbounded preceding and current row) first_known_amount_avg_6_bucket,
        round(amount_avg_6_bucket / sum(amount_avg_6_bucket) over (partition by status), 4) amount_avg_6_bucket_share,
        dense_rank() over (partition by customer_id order by amount_avg_6_avg_6 nulls last) amount_avg_6_avg_6_dense_rank,
        sum(last_known_amount_times_discount) over (partition by status order by order_date, id rows between unbounded preceding and current row) running_last_known_amount_times_discount
    from scored_orders
),
with_window as (
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from enriched src
    left join promotions pr
        on src.channel <=> pr.channel
       and src.order_date between pr.starts_on and pr.ends_on
),
with_windows as (
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        amount_times_discount,
        channel_clean,
        amount_times_discount_avg_2_prior,
        amount_avg_6,
        amount_avg_6_avg_6,
        last_known_amount_times_discount,
        amount_avg_6_bucket,
        next_amount_times_discount,
        event_seq,
        first_known_amount_avg_6_bucket,
        amount_avg_6_bucket_share,
        amount_avg_6_avg_6_dense_rank,
        running_last_known_amount_times_discount,
        promo_id,
        promo_code,
        discount_pct,
        cume_dist() over (partition by promo_code, customer_id order by last_known_amount_times_discount desc nulls first) last_known_amount_times_discount_cume_dist,
        count(next_amount_times_discount) over (partition by channel) channel_count_next_amount_times_discount,
        lead(amount_avg_6_avg_6_dense_rank, 1) over (partition by promo_code order by amount_avg_6, id, promo_id) next_amount_avg_6_avg_6_dense_rank,
        row_number() over (partition by promo_code, customer_id order by last_known_amount_times_discount, id, promo_id) seq_no
    from with_window
)
select
    customer_id,
    avg(event_seq) filter (where channel = 'partner') as partner_avg_event_seq,
    count_if(status = 'PENDING') as pending_events,
    avg(event_seq) as avg_event_seq,
    size(collect_set(channel)) as num_channels,
    round(avg(amount_avg_6_bucket), 2) as avg_amount_avg_6_bucket
from with_windows
group by customer_id
order by partner_avg_event_seq nulls first
