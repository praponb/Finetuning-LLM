select * from (select country, city, rating from suppliers)
pivot (max(rating) for city in ('Chiang Mai', 'Berlin'))
