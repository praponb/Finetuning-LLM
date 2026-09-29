select * from (select city, country, rating from suppliers) pivot (avg(rating) for country in ('UK', 'TH')) order by city
