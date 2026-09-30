select
    city,
    region,
    case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort,
    round(avg(sqft), 2) as avg_sqft,
    min(sqft) as min_sqft
from stores
where sqft > 6065 and sqft between 8906 and 12095
group by city, region, case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end
having min(sqft) < 5900
order by min_sqft desc
