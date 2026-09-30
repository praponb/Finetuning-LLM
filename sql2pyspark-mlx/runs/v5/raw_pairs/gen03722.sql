with scored as (
    -- Ranking and gap features
    select
        id,
        store_name,
        region,
        lead(sqft) over w_ordered as next_sqft,
        row_number() over w_ordered as row_seq
    from stores
    window w_ordered as (partition by city order by opened_date, id)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from inventory x
            where x.store_id = s.id
              and x.product_id in (4, 5, 6, 9)
        )
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        region,
        next_sqft,
        row_seq,
        upper(trim(store_name)) as store_name_normalized,
        coalesce(region, 'OTHER') as region_clean
    from kept
    where region not in ('Central', 'North', 'West')
),
metrics as (
    -- Ranking and gap features
    select
        *,
        sum(next_sqft) over (partition by region order by row_seq, id rows between unbounded preceding and current row) as running_next_sqft,
        avg(next_sqft) over (partition by region order by next_sqft, id rows between 6 preceding and current row) as next_sqft_avg_6,
        last_value(row_seq, true) over (partition by region order by row_seq, id rows between unbounded preceding and current row) as last_known_row_seq
    from filtered
    where row_seq >= 7
),
scored_stores as (
    -- Window metrics over several different partitions
    select
        *,
        round(next_sqft / sum(next_sqft) over (partition by region), 4) as next_sqft_share,
        ntile(5) over (partition by region order by running_next_sqft desc, id) as running_next_sqft_bucket,
        avg(next_sqft_avg_6) over (partition by region order by row_seq, id rows between 5 preceding and current row) as next_sqft_avg_6_avg_5,
        sum(row_seq) over (partition by region order by next_sqft_avg_6, id rows between unbounded preceding and current row) as running_row_seq,
        sum(last_known_row_seq) over (partition by region order by running_next_sqft, id rows between unbounded preceding and current row) as cumulative_last_known_row_seq
    from metrics
),
latest_match as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.region,
        s.next_sqft,
        s.store_name_normalized,
        s.running_next_sqft,
        s.next_sqft_avg_6,
        s.last_known_row_seq,
        s.next_sqft_share,
        s.running_next_sqft_bucket,
        s.next_sqft_avg_6_avg_5,
        top1.inventory_quantity
    from scored_stores s
    join lateral (
            select c.quantity as inventory_quantity
            from inventory c
            where c.store_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) top1 on true
),
filtered_stores as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        next_sqft,
        store_name_normalized,
        running_next_sqft,
        last_known_row_seq,
        next_sqft_share,
        running_next_sqft_bucket,
        next_sqft_avg_6_avg_5,
        inventory_quantity,
        next_sqft_avg_6_avg_5 - next_sqft_share as next_sqft_avg_6_avg_5_minus_next_sqft_share,
        coalesce(region, 'OTHER') as resolved_region
    from latest_match
    where next_sqft_share < 0.32
)
-- Window metrics over several different partitions
select
    id,
    next_sqft,
    store_name_normalized,
    last_known_row_seq,
    running_next_sqft_bucket,
    next_sqft_avg_6_avg_5,
    inventory_quantity,
    resolved_region,
    sum(inventory_quantity) over w_part as last7_sum_inventory_quantity,
    count(inventory_quantity) over w_part as last7_count_inventory_quantity,
    min(inventory_quantity) over w_part as last7_min_inventory_quantity
from filtered_stores
window w_part as (order by next_sqft_share, id rows between 6 preceding and current row);
