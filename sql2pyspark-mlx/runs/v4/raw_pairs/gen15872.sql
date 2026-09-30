with filtered as (
    select *, length(city) as city_length
    from stores
    where city = 'Singapore'
)
select
    *,
    ntile(10) over (partition by region, city order by sqft asc, id asc) as sqft_bucket,
    row_number() over (partition by region, city order by city_length desc, id asc) as row_seq
from filtered;
