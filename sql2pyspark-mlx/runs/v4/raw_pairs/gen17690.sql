with filtered as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue + units revenue_per_unit
    from store_sales
    where revenue between 1700 and 3600
),
matched_keys as (
    -- Set difference on keys
    select id, product_id
    from filtered
    except all
    select id, product_id
    from store_sales
    where revenue is null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join matched_keys k on s.id = k.id and s.product_id = k.product_id
),
with_dims as (
    -- Attach products attributes
    select
        e.*,
        d.price,
        d.category,
        d.launch_date
    from reconciled e
    inner join products d
        on e.product_id = d.id
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        corr(revenue, units) over w_part corr_revenue_units,
        corr(units, revenue) over w_part corr_units_revenue
    from with_dims
    window w_part as (partition by product_id)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        count(corr_revenue_units) over (partition by category) category_count_corr_revenue_units,
        rank() over w_hist price_rank,
        percent_rank() over (partition by product_id order by revenue desc nulls last) revenue_percent_rank
    from with_windows
    window w_hist as (order by price nulls last)
),
with_dims_store_sales as (
    -- Attach products attributes
    select a.*, d.launch_date as product_launch_date
    from metrics a
    inner join products d
        on a.product_id = d.id
),
with_windows_store_sales as (
    -- Ranking and gap features
    select
        id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_per_unit,
        price,
        category,
        launch_date,
        corr_revenue_units,
        corr_units_revenue,
        category_count_corr_revenue_units,
        price_rank,
        revenue_percent_rank,
        product_launch_date,
        cume_dist() over (order by revenue desc nulls last) revenue_cume_dist,
        max(category_count_corr_revenue_units) over (partition by product_id) product_id_max_category_count_corr_revenue_units
    from with_dims_store_sales
)
-- Grouped metrics
select
    category,
    case when revenue >= 3100 then 'LARGE' else 'SMALL' end as revenue_tier,
    count(*) as num_rows,
    round(sum(revenue) / nullif(count(case when category = 'Books' then 1 end), 0), 2) as revenue_per_books,
    min(corr_revenue_units) as min_corr_revenue_units
from with_windows_store_sales
group by category, case when revenue >= 3100 then 'LARGE' else 'SMALL' end;
