with with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.supplier_name,
        latest.cost,
        latest.price
    from suppliers s
    left join lateral (
            select c.cost, c.price
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) latest on true
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        count(price) over w_running as running_count_price,
        sum(price) over w_running as running_sum_price,
        max(price) over w_running as running_max_price,
        first_value(cost, true) over (order by cost, id rows between unbounded preceding and current row) as first_known_cost
    from with_latest
    where supplier_name like '%Traders%'
    window w_running as (order by price, id rows between unbounded preceding and current row)
),
kept_keys as (
    -- Set difference on keys
    select id
    from enriched
    except distinct
    select id
    from suppliers
    where supplier_name like '%Wholes%'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join kept_keys k on s.id = k.id
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from verified s
    where exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.supplier_id in (27, 35)
        )
)
select
    id,
    cost,
    price,
    running_count_price
from eligible
