select
    *,
    round(rating / sum(rating) over (partition by city), 4) as rating_share,
    dense_rank() over w_recent as rating_dense_rank
from suppliers
where supplier_name like '%Parts%'
window w_recent as (order by rating desc nulls first)
