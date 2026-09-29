with ranked as (
    -- QUALIFY-style window filter
    select *
    from order_items
    qualify row_number() over (partition by order_id order by quantity desc, id) = 1
)
-- Grouped metrics
select
    product_id,
    case when unit_price >= 330 then 'PREMIUM' else 'STANDARD' end as unit_price_tier,
    stddev_samp(unit_price) as unit_price_stddev,
    count(*) as cnt,
    percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles
from ranked
group by product_id, case when unit_price >= 330 then 'PREMIUM' else 'STANDARD' end
order by product_id;
