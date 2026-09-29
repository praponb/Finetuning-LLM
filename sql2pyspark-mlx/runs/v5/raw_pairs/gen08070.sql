with ranked as (
    select
        id,
        city,
        rating,
        country,
        rank() over (partition by city order by rating desc) as rnk
    from suppliers
)
select *
from ranked
where rnk <= 3
