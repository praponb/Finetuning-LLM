select *
from (
    select
        country,
        city,
        age
    from customers
) src
pivot (
    sum(age)
    for city in ('Chiang Mai' as chiang_mai, 'London' as london)
);
