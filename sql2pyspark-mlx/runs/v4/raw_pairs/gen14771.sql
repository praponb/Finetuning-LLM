select
    quarter(opened_date) opened_quarter,
    city,
    sum(case when city = 'Chiang Mai' then sqft else 0 end) chiang_mai_sqft,
    min(sqft) min_sqft
from stores
where store_name like 'Dow%'
group by quarter(opened_date), city;
