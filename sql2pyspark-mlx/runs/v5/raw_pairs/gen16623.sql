select *
from (
    select
        city,
        country,
        rating
    from suppliers
) src
pivot (
    max(rating) as amount
    for country in ('UK' as uk, 'JP' as jp, 'TH' as th)
)
