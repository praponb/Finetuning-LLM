with numbered as (
    select
        id,
        city,
        rating,
        row_number() over (partition by city order by rating desc, id) as pos
    from suppliers
)
select id, city, rating
from numbered
where pos <= 3;
