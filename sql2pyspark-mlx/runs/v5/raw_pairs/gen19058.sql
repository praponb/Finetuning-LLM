select id, country, rating from (select id, country, rating, row_number() over (partition by country order by rating desc, id) as pos from suppliers) as r where pos <= 1
