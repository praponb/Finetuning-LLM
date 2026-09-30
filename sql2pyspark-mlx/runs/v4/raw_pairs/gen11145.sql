with numbered as (
    select *, row_number() over (partition by country order by rating desc, id) row_num
    from suppliers
)
select id, country, rating
from numbered
where row_num <= 3;
