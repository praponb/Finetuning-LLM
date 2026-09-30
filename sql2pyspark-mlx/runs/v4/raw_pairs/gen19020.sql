select id, country, rating, dense_rank() over (partition by country order by rating) as rating_dense_rank, row_number() over (order by rating desc, id) as rn from suppliers;
