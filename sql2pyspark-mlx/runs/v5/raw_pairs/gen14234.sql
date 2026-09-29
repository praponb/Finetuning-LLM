with deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by store_id, product_id order by revenue desc nulls first, id) as row_num
    from store_sales
    qualify row_num = 1
)
-- Grouped metrics
select
    case when revenue >= 3900 then 'LARGE' else 'SMALL' end as revenue_tier,
    date_trunc('week', sale_date) as week_start,
    stddev_samp(revenue) as revenue_stddev,
    count(*) as event_count
from deduped
group by case when revenue >= 3900 then 'LARGE' else 'SMALL' end, date_trunc('week', sale_date);
