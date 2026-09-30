select
    country,
    case when rating >= 4.3 then 'PREMIUM' else 'STANDARD' end as rating_tier,
    approx_percentile(rating, 0.5) as rating_p50,
    count(*) as event_count,
    round(sum(rating) / nullif(count(case when country = 'JP' then 1 end), 0), 2) as rating_per_jp,
    stddev_samp(rating) as rating_stddev
from suppliers
group by country, rating_tier
order by rating_tier desc;
