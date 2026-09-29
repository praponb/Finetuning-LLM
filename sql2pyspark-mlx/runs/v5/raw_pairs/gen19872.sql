with enriched as (
    -- Attach stores attributes
    select
        src.*,
        p.store_name,
        p.city
    from orders src
    left join stores p
        on src.store_id <=> p.id
    where lower(p.store_name) like '%ive%'
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by status, channel order by order_date desc, id) as rank_in_group
    from enriched
    qualify rank_in_group = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        store_id,
        status,
        channel,
        rank_in_group,
        row_number() over (partition by store_id, status order by discount, id) as row_seq,
        last_value(city) ignore nulls over (order by discount, id rows between unbounded preceding and current row) as last_known_city
    from deduped
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_windows s
    where exists (
            select 1
            from order_items ch
            where ch.order_id = s.id
              and ch.unit_price > 300
        )
),
not_flagged_orders as (
    -- Anti-join via a correlated subquery
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from payments c
            where c.order_id = s.id
              and c.paid_at < '2024-03-01'
        )
),
latest_match as (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.payment_method,
        latest.payment_paid_at
    from not_flagged_orders s
    join lateral (
            select c.method as payment_method, c.paid_at as payment_paid_at
            from payments c
            where c.order_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) latest on true
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(payment_paid_at) over (partition by customer_id order by payment_paid_at, id) is null
                 or payment_paid_at > lag(payment_paid_at) over (partition by customer_id order by payment_paid_at, id) + interval 30 days then 1 else 0 end as is_new_burst
    from latest_match
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by customer_id order by payment_paid_at, id rows between unbounded preceding and current row) as session_id
    from gaps
)
-- Join a dimension and aggregate
select
    c.segment,
    a.status,
    count(*) as num_rows,
    min(a.row_seq) as min_row_seq,
    avg(a.is_new_burst) as avg_is_new_burst,
    grouping_id() as grouping_level
from sessionized a
left join customers c on a.customer_id = c.id
group by grouping sets ((c.segment, a.status), (c.segment), ())
order by status nulls first
