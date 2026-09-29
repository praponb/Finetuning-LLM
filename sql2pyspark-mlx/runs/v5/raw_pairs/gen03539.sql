with enriched as (
    select
        id,
        name,
        country,
        segment,
        is_active,
        sum(age) over (partition by country order by signup_date, id rows between unbounded preceding and current row) as cumulative_age,
        first_value(segment) ignore nulls over (partition by city order by age, id rows between unbounded preceding and current row) as first_known_segment,
        avg(credit_limit) over (partition by country order by age, id rows between 3 preceding and 1 preceding) as credit_limit_avg_3_prior
    from customers
)
select
    id,
    country,
    segment,
    is_active,
    cumulative_age,
    first_known_segment,
    case when country rlike '^(TH|JP)$' then 'CORE' when country rlike '^(US|SG)$' then 'GROUP_B' else 'OTHER_ZONE' end as country_zone
from enriched;
