with matched_keys as (
    -- Set intersection on keys
    select city, country
    from suppliers
    intersect all
    select city, country
    from suppliers
    where rating < 4.0
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from suppliers s
    join matched_keys k on s.city = k.city and s.country = k.country
)
select
    id,
    supplier_name,
    city,
    rating,
    case when city rlike '^(Bangkok|New York)$' then 'EXTENDED' when city rlike '^(Chiang Mai|Singapore)$' then 'CORE' else 'GROUP_B' end as city_zone
from verified
order by city desc nulls last;
