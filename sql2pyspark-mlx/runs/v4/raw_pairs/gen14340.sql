with enriched as (
    -- Attach products attributes
    select a.*, p.category
    from store_sales a
    inner join products p
        on a.product_id = p.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(revenue, true) over (partition by store_id, category order by units, id rows between unbounded preceding and current row) as first_known_revenue,
        ntile(10) over (order by units, id) as units_bucket,
        ntile(5) over (order by revenue desc, id) as revenue_bucket,
        avg(revenue) over (partition by category order by revenue, id rows between 5 preceding and 1 preceding) as revenue_avg_5_prior
    from enriched
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by product_id order by units_bucket desc nulls last, id) as dedup_rank
    from metrics
    qualify dedup_rank = 1
),
enriched_store_sales as (
    -- Window metrics over several different partitions
    select
        *,
        avg(revenue_avg_5_prior) over (partition by product_id order by revenue_bucket, id rows between 5 preceding and current row) as revenue_avg_5_prior_avg_5,
        lag(units_bucket, 2) over (partition by category order by sale_date, id) as prev_units_bucket
    from ranked
),
scored as (
    -- Ranking and gap features
    select
        *,
        sum(revenue_bucket) over (order by units, id rows between unbounded preceding and current row) as running_revenue_bucket,
        row_number() over (partition by store_id order by revenue_avg_5_prior, id) as seq_no,
        first_value(units_bucket) ignore nulls over (partition by category, store_id order by first_known_revenue, id rows between unbounded preceding and current row) as first_known_units_bucket
    from enriched_store_sales
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by product_id, category order by first_known_units_bucket desc, id) as rank_in_group
    from scored
    qualify rank_in_group = 1
)
select
    s.id,
    s.store_id,
    s.product_id,
    s.sale_date,
    s.units,
    s.revenue,
    s.first_known_revenue,
    s.units_bucket,
    s.revenue_bucket,
    (select avg(x.quantity) from order_items x where x.product_id = s.product_id) as avg_order_item_quantity
from deduped s;
