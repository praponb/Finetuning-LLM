with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity - unit_price line_total
    from order_items
    where product_id is not null
),
remaining_keys as (
    -- Set difference on keys
    select product_id
    from cleaned
    except distinct
    select product_id
    from order_items
    where order_id in (19, 22)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join remaining_keys k on s.product_id = k.product_id
),
enriched as (
    -- Attach orders attributes
    select
        a.*,
        d.status,
        d.order_date,
        d.discount
    from verified a
    left join orders d
        on a.order_id = d.id
),
scored as (
    -- Ranking and gap features
    select
        *,
        count(discount) over (partition by order_id, product_id) order_id_count_discount,
        cume_dist() over (partition by product_id order by discount asc) discount_cume_dist,
        round(unit_price / sum(unit_price) over (partition by product_id, status), 4) unit_price_share
    from enriched
),
daily as (
    -- Grouped metrics
    select
        date_trunc('week', order_date) as week_start,
        date_trunc('month', order_date) as activity_month,
        count_if(status = 'SHIPPED') as shipped_events,
        sum(order_id_count_discount) filter (where status = 'PENDING') as pending_total_order_id_count_discount,
        percentile_approx(discount_cume_dist, 0.9) as discount_cume_dist_p90
    from scored
    group by 1, 2
)
select
    week_start,
    activity_month,
    pending_total_order_id_count_discount,
    discount_cume_dist_p90
from daily
order by week_start asc, activity_month desc;
