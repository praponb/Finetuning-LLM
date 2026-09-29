select * from (select city, country, rating from suppliers)
pivot (avg(rating) for country in ('TH', 'SG'))
