select
    city,
    case when rating >= 3.5 then 'high' when rating >= 2.4 then 'mid' else 'low' end as rating_bucket,
    max(rating) - min(rating) as rating_range,
    sum(case when country = 'SG' then 1 else 0 end) as sg_count
from suppliers
group by city, case when rating >= 3.5 then 'high' when rating >= 2.4 then 'mid' else 'low' end;
