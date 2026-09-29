with valid as (
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        coalesce(units, 0) as units_filled
    from store_sales
    where sale_date >= to_date('2023-08-17')
),
with_windows as (
    select
        *,
        sum(units) over (partition by product_id, store_id order by units_filled, id rows between unbounded preceding and current row) as running_units,
        lead(units_filled, 2) over (partition by store_id order by units_filled, id) as next_units_filled,
        lag(units_filled, 2) over (partition by store_id order by units, id) as prev_units_filled
    from valid
),
matched_keys as (
    select product_id
    from with_windows
    except
    select product_id
    from store_sales
    where sale_date >= '2024-04-01'
),
verified as (
    select s.*
    from with_windows s
    inner join matched_keys k on s.product_id = k.product_id
),
with_windows_store_sales as (
    select
        *,
        round(units / sum(units) over (partition by product_id), 4) as units_share,
        stddev_samp(units) over (partition by store_id) as store_id_stddev_samp_units,
        cume_dist() over (order by units_filled) as units_filled_cume_dist,
        sum(running_units) over (partition by product_id, store_id) as product_id_sum_running_units,
        round(units / sum(units) over (partition by store_id), 4) as units_share_2
    from verified
),
with_dims as (
    select
        s.*,
        p.stock_qty,
        p.brand
    from with_windows_store_sales s
    left join products p
        on s.product_id = p.id
),
valid_store_sales as (
    select *, cast(units_share as int) as units_share_int
    from with_dims
    where brand is not null
),
with_dims_store_sales as (
    select
        e.*,
        p.product_name,
        p.launch_date
    from valid_store_sales e
    inner join products p
        on e.product_id = p.id
)
select
    store_id,
    sale_date,
    units_filled,
    running_units,
    prev_units_filled,
    units_share,
    units_filled_cume_dist,
    product_id_sum_running_units,
    units_share_2
from with_dims_store_sales;
