select * from (select city, country, rating from suppliers)
pivot (avg(rating) for country in ('DE', 'JP', 'US', 'TH'))
order by city
