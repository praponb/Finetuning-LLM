with filtered as (
    select
        *,
        regexp_extract(email, '@(.+)$', 1) as email_domain,
        coalesce(city, 'OTHER') as city_clean
    from customers
    where city is not distinct from 'Tokyo'
)
select id, segment
from filtered
intersect all
select id, segment
from customers
where email not like '%@startup.io';
