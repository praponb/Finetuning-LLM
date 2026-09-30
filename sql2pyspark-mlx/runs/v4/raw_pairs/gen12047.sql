with scored as (
    select
        *,
        avg(rating) over (partition by country order by rating, id rows between 2 preceding and 1 preceding) as rating_avg_2_prior,
        stddev_samp(rating) over (partition by country) as country_stddev_samp_rating,
        sum(rating) over (partition by city order by rating, id rows between unbounded preceding and current row) as cumulative_rating,
        round(rating / sum(rating) over (partition by country), 4) as rating_share,
        last_value(city) ignore nulls over (partition by country order by rating, id rows between unbounded preceding and current row) as last_known_city
    from suppliers
)
select
    country,
    case when country_stddev_samp_rating >= 4 then 'HIGH' else 'LOW' end as country_stddev_samp_rating_tier,
    count(distinct country) as distinct_countries,
    round(sum(rating_avg_2_prior) / nullif(count(case when city = 'Singapore' then 1 end), 0), 2) as rating_avg_2_prior_per_singapore,
    round(sum(rating_avg_2_prior) / nullif(count(case when country = 'TH' then 1 end), 0), 2) as rating_avg_2_prior_per_th,
    sum(rating_avg_2_prior) filter (where city = 'Berlin') as berlin_total_rating_avg_2_prior,
    round(avg(cumulative_rating), 2) as avg_cumulative_rating
from scored
group by rollup (country, case when country_stddev_samp_rating >= 4 then 'HIGH' else 'LOW' end)
having round(avg(cumulative_rating), 2) > 0
order by rating_avg_2_prior_per_th desc nulls last;
