with enriched as (
    select a.*, b.cost
    from order_items a
    left join products b
        on a.product_id <=> b.id
),
enriched_order_items as (
    select
        *,
        corr(quantity, unit_price) over (partition by product_id) corr_quantity_unit_price,
        avg(quantity) over (order by unit_price, id rows between 5 preceding and current row) quantity_avg_5
    from enriched
),
scored as (
    select
        *,
        row_number() over w_running row_seq,
        max(corr_quantity_unit_price) over (partition by order_id) order_id_max_corr_quantity_unit_price
    from enriched_order_items
    where product_id in (3, 36)
    window w_running as (order by corr_quantity_unit_price, id)
),
remaining_keys as (
    select order_id
    from scored
    except
    select order_id
    from order_items
    where product_id = 9
),
verified as (
    select s.*
    from scored s
    inner join remaining_keys k on s.order_id = k.order_id
),
enriched_order_items_2 as (
    select
        src.*,
        d.product_name,
        d.stock_qty
    from verified src
    left join products d
        on src.product_id = d.id
),
cleaned as (
    select
        order_id,
        product_id,
        quantity,
        unit_price,
        cost,
        quantity_avg_5,
        row_seq,
        order_id_max_corr_quantity_unit_price,
        product_name,
        stock_qty,
        cast(unit_price as int) unit_price_int
    from enriched_order_items_2
    where quantity is not null
),
scored_order_items as (
    select
        *,
        max(unit_price_int) over (partition by product_id, order_id) product_id_max_unit_price_int,
        round(quantity_avg_5 / sum(quantity_avg_5) over (partition by order_id), 4) quantity_avg_5_share,
        cume_dist() over (partition by product_id, order_id order by unit_price_int nulls first) unit_price_int_cume_dist,
        stddev_samp(quantity_avg_5) over w_running product_id_stddev_samp_quantity_avg_5,
        dense_rank() over (partition by product_id order by order_id_max_corr_quantity_unit_price) order_id_max_corr_quantity_unit_price_dense_rank
    from cleaned
    window w_running as (partition by product_id)
)
select
    product_id,
    kpi,
    val
from (
    select
        product_id,
        unit_price_int_cume_dist,
        cost,
        unit_price
    from scored_order_items
) src
unpivot include nulls (
    val for kpi in (unit_price_int_cume_dist, cost, unit_price)
) u;
