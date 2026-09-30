with deduped as (
    select
        id,
        store_id,
        product_id,
        sale_date,
        revenue
    from (
        select *, rank() over (partition by store_id order by units desc) as row_num
        from store_sales
    ) ranked
    where row_num = 1
)
select
    date_trunc('week', sale_date) as week_start,
    stddev_samp(revenue) as revenue_stddev,
    stddev_samp(revenue) as revenue_stddev_2,
    stddev_samp(revenue) as revenue_stddev_3
from deduped
group by date_trunc('week', sale_date)
having revenue_stddev_2 > 0
order by revenue_stddev_2 desc;
