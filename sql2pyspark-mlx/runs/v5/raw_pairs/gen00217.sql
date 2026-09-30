select *
from (
    select
        region,
        city,
        sqft
    from stores
) src
pivot (
    max(sqft)
    for city in ('Bangkok', 'Berlin')
)
