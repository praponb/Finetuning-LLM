with with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select s.*, top1.quantity
    from orders s
    left join lateral (
            select c.quantity
            from order_items c
            where c.order_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) top1 on true
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from with_latest
    qualify row_number() over (partition by store_id, channel order by discount desc, id asc) = 1
),
matched as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from ranked e
    left join promotions pr
        on e.channel = pr.channel
       and e.order_date >= pr.starts_on
       and e.order_date < pr.ends_on + interval '30' day
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from matched s
    where exists (
            select 1
            from shipments c
            where c.order_id = s.id
              and c.shipped_date <= s.order_date
        )
),
kept_keys as (
    -- Set intersection on keys
    select status, id
    from kept
    intersect all
    select status, id
    from orders
    where amount between 940 and 1270
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept s
    inner join kept_keys k on s.status = k.status and s.id = k.id
)
select
    customer_id,
    store_id,
    channel,
    amount,
    discount,
    shipping_fee,
    promo_id,
    discount_pct
from kept_rows
order by promo_id desc
