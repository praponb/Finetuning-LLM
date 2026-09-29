select
    case when year(p.launch_date) < 2024 then 'before_2024' when year(p.launch_date) = 2024 then '2024' else 'after_2024' end as launch_cohort,
    p.brand,
    count(*) as cnt,
    max(ss.sale_date) as last_sale_date
from store_sales ss
right join products p on p.id = ss.product_id
group by case when year(p.launch_date) < 2024 then 'before_2024' when year(p.launch_date) = 2024 then '2024' else 'after_2024' end, p.brand
having count(*) > 4
