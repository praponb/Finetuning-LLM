with cleaned as (
    -- Keep recent, valid rows
    select *, upper(country) as country_upper
    from customers
    where segment is distinct from 'Corporate'
      and signup_date > to_date('2023-08-26')
)
-- Keep one row per key (latest first)
select
    id,
    city,
    segment
from cleaned
qualify row_number() over (partition by city, segment order by signup_date desc nulls last, id) = 1
