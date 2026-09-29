with with_dims as (
    -- Attach orders attributes
    select
        src.*,
        b.amount,
        b.discount
    from order_items src
    inner join orders b
        on src.order_id = b.id
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by product_id order by discount desc nulls last, id) as row_num
        from with_dims
    ) x
    where row_num = 1
),
matched_keys as (
    -- Set difference on keys
    select order_id, product_id
    from latest
    except distinct
    select order_id, product_id
    from order_items
    where order_id in (5, 32, 36)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    inner join matched_keys k on s.order_id = k.order_id and s.product_id = k.product_id
)
-- Multi-level aggregation
select
    order_id,
    case when discount >= 0.37 then 'HIGH' else 'LOW' end as discount_tier,
    stddev_samp(quantity) as quantity_stddev,
    approx_percentile(discount, 0.9) as discount_p90,
    avg(discount) as avg_discount
from reconciled
group by grouping sets ((order_id, case when discount >= 0.37 then 'HIGH' else 'LOW' end), (order_id))
order by avg_discount desc
