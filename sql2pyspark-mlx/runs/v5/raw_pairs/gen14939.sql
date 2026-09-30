with enriched as (
    -- Window metrics over several different partitions
    select
        id,
        store_name,
        sqft,
        avg(sqft) over (partition by region order by sqft, id rows between 2 preceding and current row) sqft_avg_2,
        row_number() over (order by sqft desc, id) event_seq,
        lead(sqft, 1) over (partition by region order by opened_date, id) next_sqft,
        sum(sqft) over (partition by region order by sqft, id rows between unbounded preceding and current row) cumulative_sqft
    from stores
    where length(store_name) > 9
),
with_latest as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select s.*, x.inventory_quantity
    from enriched s
    left join lateral (
            select c.quantity as inventory_quantity
            from inventory c
            where c.store_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) x on true
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        sqft,
        sqft_avg_2,
        inventory_quantity,
        sum(event_seq) over (order by inventory_quantity, id rows between unbounded preceding and current row) running_event_seq,
        cume_dist() over (order by inventory_quantity desc nulls last) inventory_quantity_cume_dist,
        first_value(sqft_avg_2) ignore nulls over (order by next_sqft, id rows between unbounded preceding and current row) first_known_sqft_avg_2,
        avg(event_seq) over (order by sqft, id rows between 5 preceding and 1 preceding) event_seq_avg_5_prior,
        sum(inventory_quantity) over (order by event_seq, id rows between unbounded preceding and current row) cumulative_inventory_quantity
    from with_latest
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.customer_id is not null
        )
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from store_sales c
            where c.store_id = s.id
              and c.store_id in (2, 3, 4, 5)
        )
)
-- Grouped metrics
select
    case when event_seq_avg_5_prior >= 7 then 'PREMIUM' else 'STANDARD' end as event_seq_avg_5_prior_tier,
    percentile_approx(running_event_seq, 0.5) as running_event_seq_p50,
    count(distinct id) as distinct_ids
from kept
group by 1
order by event_seq_avg_5_prior_tier nulls last
