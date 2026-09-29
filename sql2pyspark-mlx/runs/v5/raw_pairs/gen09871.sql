select * from (select category, brand, price from products)
pivot (count(price) for brand in ('Globex', 'Umbrella'))
order by category
