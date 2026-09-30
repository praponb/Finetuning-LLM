with latest as (
    -- QUALIFY-style window filter
    select *
    from products
    qualify rank() over (partition by brand order by launch_date desc nulls first) = 1
),
remaining_keys as (
    -- Set difference on keys
    select category
    from latest
    except distinct
    select category
    from products
    where cost between 370 and 540
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    join remaining_keys k on s.category = k.category
),
metrics as (
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
        dense_rank() over (partition by brand order by price desc nulls last) price_dense_rank,
        round(cost / sum(cost) over (partition by category, supplier_id), 4) cost_share
    from reconciled
),
remaining_keys_2 as (
    -- Set difference on keys
    select id, category
    from metrics
    except
    select id, category
    from products
    where brand != 'Globex'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    join remaining_keys_2 k on s.id = k.id and s.category = k.category
),
totals as (
    -- Aggregate with conditional metrics
    select
        case when stock_qty >= 392 then 'PREMIUM' else 'STANDARD' end as stock_qty_tier,
        approx_percentile(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
        sort_array(collect_set(category)) as category_list,
        round(sum(cost) / nullif(count(case when brand = 'Acme' then 1 end), 0), 2) as cost_per_acme,
        stddev_samp(price) as price_stddev
    from verified
    group by 1
)
select
    stock_qty_tier,
    cost_quartiles,
    category_list,
    cost_per_acme,
    price_stddev
from totals
order by price_stddev nulls first, cost_per_acme desc;
