with latest_match as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select s.*, x.amount
    from stores s
    left join lateral (
            select c.amount
            from orders c
            where c.store_id = s.id
            order by c.order_date desc, c.id desc
            limit 1
        ) x on true
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from latest_match
    qualify rank() over (partition by region order by opened_date desc nulls first) = 1
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from deduped
    qualify row_number() over (partition by city order by opened_date desc, id) = 1
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from latest s
    where exists (
            select 1
            from store_sales ch
            where ch.store_id = s.id
              and ch.sale_date >= s.opened_date
        )
),
grouped as (
    -- Aggregate with conditional metrics
    select
        case when amount >= 1630 then 'LARGE' else 'SMALL' end as amount_tier,
        count(*) as cnt,
        count(distinct id) as distinct_ids,
        size(collect_set(region)) as num_regions,
        grouping_id() as gid
    from kept
    group by cube (case when amount >= 1630 then 'LARGE' else 'SMALL' end)
)
select
    amount_tier,
    cnt,
    distinct_ids,
    num_regions
from grouped
order by num_regions desc;
