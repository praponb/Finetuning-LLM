with filtered as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        quantity,
        last_day(last_restock) as last_restock_month_end,
        datediff(current_date(), last_restock) as days_since_last_restock
    from inventory
    where last_restock > date '2023-09-06'
),
matched_keys as (
    -- Set difference on keys
    select id, product_id
    from filtered
    except all
    select id, product_id
    from inventory
    where store_id = 3
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join matched_keys k on s.id = k.id and s.product_id = k.product_id
),
scored as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by product_id, store_id order by quantity desc) as quantity_dense_rank,
        avg(days_since_last_restock) over (partition by product_id, store_id) as product_id_avg_days_since_last_restock,
        dense_rank() over (order by quantity desc nulls last) as quantity_dense_rank_2,
        cume_dist() over w_ordered as days_since_last_restock_cume_dist
    from kept_rows
    window w_ordered as (partition by product_id, store_id order by days_since_last_restock asc)
),
scored_inventory as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock_month_end,
        days_since_last_restock,
        quantity_dense_rank,
        product_id_avg_days_since_last_restock,
        quantity_dense_rank_2,
        days_since_last_restock_cume_dist,
        round(quantity_dense_rank_2 / sum(quantity_dense_rank_2) over (partition by product_id), 4) as quantity_dense_rank_2_share,
        min(days_since_last_restock) over (partition by store_id) as store_id_min_days_since_last_restock,
        round(quantity_dense_rank_2 / sum(quantity_dense_rank_2) over (partition by store_id), 4) as quantity_dense_rank_2_share_2
    from scored
),
filtered_inventory as (
    -- Filter the raw rows and derive helper columns
    select *, round(product_id_avg_days_since_last_restock, 0) as product_id_avg_days_since_last_restock_rounded
    from scored_inventory
    where quantity_dense_rank_2_share between 0.21 and 0.74
),
joined as (
    -- Attach stores attributes
    select
        a.*,
        p.region,
        p.sqft,
        p.opened_date
    from filtered_inventory a
    inner join stores p
        on a.store_id = p.id
)
select
    id,
    store_id,
    quantity,
    last_restock_month_end,
    days_since_last_restock,
    product_id_avg_days_since_last_restock,
    quantity_dense_rank_2,
    days_since_last_restock_cume_dist,
    quantity_dense_rank_2_share
from joined
order by days_since_last_restock_cume_dist desc;
