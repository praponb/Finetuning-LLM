select id, country, rating, ntile(4) over (partition by country order by rating, id) as bucket from suppliers
