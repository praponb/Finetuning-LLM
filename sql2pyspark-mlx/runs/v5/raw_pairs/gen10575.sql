select
    case when cost >= 620 then 'high' when cost >= 150 then 'mid' else 'low' end as cost_bucket,
    sum(case when category = 'Sports' then stock_qty else 0 end) as sports_stock_qty,
    round((100 * sum(case when brand = 'Initech' then 1 else 0 end)) / count(*), 1) as pct_initech,
    sum(case when category = 'Home' then 1 else 0 end) as home_count
from products
where product_name like '%Bottle%' and cost between 300 and 720
group by case when cost >= 620 then 'high' when cost >= 150 then 'mid' else 'low' end
having sum(case when category = 'Home' then 1 else 0 end) < 40
order by pct_initech
