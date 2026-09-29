with ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by city, region order by opened_date desc, id) as rank_in_group
        from stores
    ) ranked
    where rank_in_group = 1
),
latest_match as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.order_amount,
        x.order_order_date
    from ranked s
    left join lateral (
            select c.amount as order_amount, c.order_date as order_order_date
            from orders c
            where c.store_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) x on true
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by city, region order by order_order_date desc, id) as row_num
    from latest_match
    qualify row_num = 1
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from deduped s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.order_date > '2024-03-01'
        )
)
select
    id,
    store_name,
    city,
    region,
    sqft,
    rank_in_group,
    order_order_date,
    row_num,
    case when city rlike '^(Bangkok|Singapore)$' then 'EXTENDED' when city rlike '^(Chiang Mai|New York)$' then 'CORE' else 'OTHER_ZONE' end as city_zone
from kept;
