select * from (select id, country, rating, rank() over (partition by country order by rating) as rn from suppliers) as t where rn = 1 order by rn desc
