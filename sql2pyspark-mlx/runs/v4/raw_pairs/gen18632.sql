select
    *,
    row_number() over (partition by country, city order by rating desc, id) as seq_no,
    dense_rank() over (partition by country order by rating desc nulls first) as rating_dense_rank,
    percent_rank() over (partition by country, city order by rating nulls first) as rating_percent_rank
from suppliers;
