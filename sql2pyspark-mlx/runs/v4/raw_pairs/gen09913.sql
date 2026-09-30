select
    country,
    count(case when country = 'DE' then 1 end) as de_count,
    approx_percentile(rating, 0.9) as rating_p90
from suppliers
group by country
having approx_percentile(rating, 0.9) < 0
order by country
