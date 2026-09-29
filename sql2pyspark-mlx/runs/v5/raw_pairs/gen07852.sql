with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        round(cost * 100, 2) as cost_scaled,
        add_months(shipped_date, 6) as shipped_date_plus_months
    from shipments
    where delivered_date > date '2023-03-31'
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by order_id order by cost_scaled) as rank_in_group
        from filtered
    ) x
    where rank_in_group = 1
),
enriched as (
    -- Attach orders attributes
    select
        s.*,
        p.shipping_fee,
        p.channel
    from latest s
    left join orders p
        on s.order_id = p.id
),
joined as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.carrier,
        src.delivered_date,
        src.shipping_fee,
        p.status,
        p.order_date
    from enriched src
    left join orders p
        on src.order_id = p.id
),
scored as (
    -- Running and moving aggregates
    select
        id,
        carrier,
        delivered_date,
        shipping_fee,
        status,
        stddev_samp(shipping_fee) over (partition by status) as status_stddev_samp_shipping_fee,
        row_number() over (partition by status order by order_date, id) as row_seq
    from joined
),
enriched_shipments as (
    -- Running and moving aggregates
    select
        id,
        delivered_date,
        lag(status_stddev_samp_shipping_fee) over (partition by status order by status_stddev_samp_shipping_fee, id) as prev_status_stddev_samp_shipping_fee,
        lead(row_seq) over (partition by status order by status_stddev_samp_shipping_fee, id) as next_row_seq,
        lag(row_seq) over (partition by status order by status_stddev_samp_shipping_fee, id) as prev_row_seq,
        row_number() over (partition by status order by status_stddev_samp_shipping_fee, id) as row_seq_2,
        last_value(shipping_fee) ignore nulls over (partition by status, carrier order by delivered_date, id rows between unbounded preceding and current row) as last_known_shipping_fee
    from scored
)
-- Multi-level aggregation
select
    date_trunc('quarter', delivered_date) as quarter_start,
    count(*) as row_count,
    count(distinct id) as distinct_ids,
    percentile_approx(row_seq_2, 0.95) as row_seq_2_p95,
    count(distinct id) as distinct_ids_2
from enriched_shipments
group by date_trunc('quarter', delivered_date)
order by row_count desc;
