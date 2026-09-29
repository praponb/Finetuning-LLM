select * from (select case when price >= 1090 then 'high' when price >= 170 then 'mid' else 'low' end as price_bucket, category, stock_qty from products where cost > 540)
pivot (max(stock_qty) for category in ('Electronics', 'Books', 'Grocery', 'Sports'))
