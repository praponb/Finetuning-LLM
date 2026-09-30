select
    id,
    country,
    city,
    rating,
    rank() over (partition by country, city order by rating) as rating_rank
from suppliers
where upper(supplier_name) like '%IAM%'
order by rating_rank desc;
