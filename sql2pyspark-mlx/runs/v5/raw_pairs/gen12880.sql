select *
from (
    select
        region,
        city,
        sqft
    from stores
) src
pivot (
    max(sqft) as total,
    count(1) as item_count
    for city in ('Bangkok', 'Singapore')
);
