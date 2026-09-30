select * from (select case when rating >= 3.8 then 'high' when rating >= 2.1 then 'mid' else 'low' end as rating_bucket, country, rating from suppliers)
pivot (avg(rating) for country in ('UK', 'DE', 'US'));
