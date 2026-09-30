select
    city,
    month(opened_date) opened_month,
    sum(case when region = 'North' then sqft else 0 end) north_sqft,
    count(case when region = 'North' then 1 end) north_count
from stores
group by city, month(opened_date);
