select
    id,
    country,
    rating,
    city
from (
    select
        id,
        country,
        rating,
        city,
        dense_rank() over (partition by country order by rating desc) as row_num
    from suppliers
) as t
where row_num <= 3
order by country desc
