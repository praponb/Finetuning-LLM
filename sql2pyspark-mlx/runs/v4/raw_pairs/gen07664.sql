with remaining_keys as (
    -- Set difference on keys
    select brand, id
    from products
    except
    select brand, id
    from products
    where cost < 310
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from products s
    inner join remaining_keys k on s.brand = k.brand and s.id = k.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        stddev_samp(cost) over (partition by supplier_id) supplier_id_stddev_samp_cost,
        dense_rank() over (order by stock_qty nulls last) stock_qty_dense_rank,
        avg(cost) over (partition by category, supplier_id) category_avg_cost
    from verified
),
enriched as (
    -- Ranking and gap features
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        supplier_id_stddev_samp_cost,
        stock_qty_dense_rank,
        category_avg_cost,
        max(supplier_id_stddev_samp_cost) over (partition by supplier_id, category) supplier_id_max_supplier_id_stddev_samp_cost,
        percent_rank() over (partition by category order by category_avg_cost nulls last) category_avg_cost_percent_rank
    from metrics
),
with_dims as (
    -- Attach suppliers attributes
    select
        src.*,
        b.rating,
        b.city
    from enriched src
    inner join suppliers b
        on src.supplier_id = b.id
),
with_dims_products as (
    -- Attach suppliers attributes
    select src.*, d.country
    from with_dims src
    left join suppliers d
        on src.supplier_id = d.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        supplier_id_stddev_samp_cost,
        stock_qty_dense_rank,
        category_avg_cost,
        supplier_id_max_supplier_id_stddev_samp_cost,
        category_avg_cost_percent_rank,
        rating,
        city,
        country,
        rank() over (partition by category order by supplier_id_stddev_samp_cost desc) supplier_id_stddev_samp_cost_rank,
        dense_rank() over (partition by city order by supplier_id_max_supplier_id_stddev_samp_cost desc nulls last) supplier_id_max_supplier_id_stddev_samp_cost_dense_rank,
        sum(stock_qty) over (partition by supplier_id) supplier_id_sum_stock_qty,
        corr(stock_qty, price) over (partition by city) corr_stock_qty_price
    from with_dims_products
),
joined as (
    -- Attach suppliers attributes
    select
        src.*,
        s.supplier_name,
        s.city as supplier_city
    from with_windows src
    left join suppliers s
        on src.supplier_id = s.id
)
-- Running and moving aggregates
select
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    supplier_id,
    supplier_id_stddev_samp_cost,
    stock_qty_dense_rank,
    category_avg_cost,
    supplier_id_max_supplier_id_stddev_samp_cost,
    category_avg_cost_percent_rank,
    rating,
    city,
    country,
    supplier_id_stddev_samp_cost_rank,
    supplier_id_max_supplier_id_stddev_samp_cost_dense_rank,
    supplier_id_sum_stock_qty,
    corr_stock_qty_price,
    supplier_name,
    supplier_city,
    stddev_samp(supplier_id_sum_stock_qty) over (partition by city) city_stddev_samp_supplier_id_sum_stock_qty,
    round(cost / sum(cost) over (partition by country), 4) cost_share,
    dense_rank() over (partition by city order by corr_stock_qty_price desc) corr_stock_qty_price_dense_rank,
    rank() over (order by supplier_id_max_supplier_id_stddev_samp_cost_dense_rank desc) supplier_id_max_supplier_id_stddev_samp_cost_dense_rank_rank,
    sum(supplier_id_sum_stock_qty) over (partition by brand) brand_sum_supplier_id_sum_stock_qty
from joined
