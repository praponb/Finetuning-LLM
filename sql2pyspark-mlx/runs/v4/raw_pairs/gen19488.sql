with base as (
    select
        *,
        initcap(trim(region)) as region_title,
        coalesce(region, 'N/A') as resolved_region
    from stores
    where opened_date > date '2023-08-15'
)
select *
from (
    select
        region,
        city,
        sqft
    from base
) src
pivot (
    max(sqft) as total
    for city in ('Chiang Mai' as chiang_mai, 'Tokyo' as tokyo)
);
