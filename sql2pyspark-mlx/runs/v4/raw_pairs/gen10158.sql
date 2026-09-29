select
    category,
    count(*) product_count,
    count(price) price_count,
    sum(case when category = 'Home' then stock_qty else 0 end) home_stock_qty
from products
group by category;
