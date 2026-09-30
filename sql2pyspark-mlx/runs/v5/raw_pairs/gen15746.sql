with valid as (
    -- Basic cleansing and defaults
    select *, concat(segment, '/', city) as segment_city
    from customers
    where signup_date >= date '2023-05-11'
      and city is distinct from 'Chiang Mai'
)
-- Native SQL PIVOT
select *
from (
    select
        city,
        country,
        credit_limit
    from valid
) src
pivot (
    sum(credit_limit)
    for country in ('SG' as sg, 'JP' as jp, 'DE' as de)
);
