select
    d.category,
    d.brand,
    count(*) as cnt,
    round(sum(src.revenue), 2) as total_revenue,
    grouping_id() as grouping_level
from store_sales src
inner join products d on src.product_id = d.id
group by grouping sets ((d.category, d.brand), (d.category))
