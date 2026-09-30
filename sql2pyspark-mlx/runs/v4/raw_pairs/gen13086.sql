with with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        avg(unit_price) over (partition by order_id order by unit_price, id rows between 6 preceding and 1 preceding) as unit_price_avg_6_prior,
        round(unit_price / sum(unit_price) over (partition by product_id), 4) as unit_price_share,
        row_number() over (partition by order_id, product_id order by unit_price, id) as seq_no
    from order_items
)
-- Multi-level aggregation
select
    case when quantity >= 9 then 'LARGE' else 'SMALL' end as quantity_tier,
    case when unit_price_share >= 0.71 then 'LARGE' else 'SMALL' end as unit_price_share_tier,
    seq_no,
    percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    round(avg(unit_price), 2) as avg_unit_price,
    approx_percentile(unit_price_share, 0.5) as unit_price_share_p50,
    grouping(case when quantity >= 9 then 'LARGE' else 'SMALL' end) as gid
from with_windows
group by grouping sets ((case when quantity >= 9 then 'LARGE' else 'SMALL' end, case when unit_price_share >= 0.71 then 'LARGE' else 'SMALL' end, seq_no), (case when quantity >= 9 then 'LARGE' else 'SMALL' end, case when unit_price_share >= 0.71 then 'LARGE' else 'SMALL' end))
