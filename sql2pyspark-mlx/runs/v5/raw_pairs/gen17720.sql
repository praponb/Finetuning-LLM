select *
from (
    select
        id,
        city,
        opened_date,
        sqft,
        row_number() over (partition by city order by opened_date, id) as pos
    from stores
) as r
where pos <= 5
order by city desc;
