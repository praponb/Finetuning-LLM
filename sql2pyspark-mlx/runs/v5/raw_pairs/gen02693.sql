with valid as (
    -- Keep recent, valid rows
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        supplier_id,
        round(stock_qty / cost, 2) as stock_qty_per_cost,
        add_months(launch_date, 1) as launch_date_plus_months,
        coalesce(brand, 'NONE') as resolved_brand
    from products
    where brand is not null
      and launch_date >= date '2023-04-27'
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(4) over w_grp as cost_bucket,
        first_value(brand) ignore nulls over (partition by brand order by launch_date_plus_months, id rows between unbounded preceding and current row) as first_known_brand,
        avg(cost) over (partition by supplier_id) as supplier_id_avg_cost
    from valid
    window w_grp as (order by cost, id)
),
remaining_keys as (
    -- Set difference on keys
    select brand, category
    from metrics
    except all
    select brand, category
    from products
    where array_contains(tags, 'new')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join remaining_keys k on s.brand = k.brand and s.category = k.category
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        count(cost_bucket) over w_hist as brand_count_cost_bucket,
        count(cost_bucket) over w_hist_2 as category_count_cost_bucket,
        corr(cost, stock_qty) over w_hist_2 as corr_cost_stock_qty,
        sum(supplier_id_avg_cost) over w_hist_2 as category_sum_supplier_id_avg_cost,
        sum(stock_qty) over w_part as supplier_id_sum_stock_qty
    from kept_rows
    window w_hist as (partition by brand), w_hist_2 as (partition by category), w_part as (partition by supplier_id)
),
scored as (
    -- Ranking and gap features
    select
        *,
        round(supplier_id_avg_cost / sum(supplier_id_avg_cost) over (partition by supplier_id, category), 4) as supplier_id_avg_cost_share,
        round(cost_bucket / sum(cost_bucket) over (partition by brand), 4) as cost_bucket_share,
        percent_rank() over w_ordered as category_count_cost_bucket_percent_rank
    from enriched
    window w_ordered as (partition by brand order by category_count_cost_bucket)
)
-- Join a dimension and aggregate
select
    p.country,
    e.category,
    count(*) as cnt,
    round(avg(e.category_count_cost_bucket), 2) as avg_category_count_cost_bucket,
    grouping_id() as grouping_level
from scored e
left join suppliers p on e.supplier_id = p.id
group by grouping sets ((p.country, e.category), (p.country), ());
