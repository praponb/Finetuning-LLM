select
    city,
    sum(sqft) as total_sqft,
    count(*) as cnt,
    round(sum(sqft) * 100.0 / sum(sum(sqft)) over (), 2) as pct_of_total
from stores
group by city
order by cnt desc;
