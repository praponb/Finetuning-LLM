select is_active, country, uk_age
from (
    select is_active, country, sum(case when country = 'UK' then age else 0 end) as uk_age
    from customers
    group by is_active, country
) as s
where uk_age > 66;
