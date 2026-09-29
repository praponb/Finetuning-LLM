with numbered as (
    select
        id,
        city,
        sqft,
        opened_date,
        row_number() over (partition by city order by sqft desc, id) as rn
    from stores
)
select *
from numbered
where rn <= 3
