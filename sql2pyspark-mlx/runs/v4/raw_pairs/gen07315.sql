select * from (select city, country, rating from suppliers) pivot (sum(rating) for country in ('SG', 'UK', 'TH', 'JP'))
