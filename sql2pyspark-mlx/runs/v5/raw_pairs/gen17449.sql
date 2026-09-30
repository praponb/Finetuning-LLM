with enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(age) over w_part as running_age,
        round(age / sum(age) over (partition by city), 4) as age_share,
        sum(credit_limit) over (partition by country order by credit_limit, id rows between unbounded preceding and current row) as cumulative_credit_limit
    from customers
    window w_part as (partition by segment order by age, id rows between unbounded preceding and current row)
),
metrics as (
    -- Ranking and gap features
    select
        *,
        avg(running_age) over (partition by city) as city_avg_running_age,
        min(credit_limit) over (partition by country, city) as country_min_credit_limit,
        ntile(3) over (partition by segment, country order by credit_limit, id) as credit_limit_bucket,
        row_number() over (partition by country order by credit_limit desc, id) as event_seq
    from enriched
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from metrics s
    where exists (
            select 1
            from orders c
            where c.customer_id = s.id
              and c.order_date <= s.signup_date
        )
),
eligible_customers as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from orders x
            where x.customer_id = s.id
              and x.shipping_fee > 40
        )
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        city,
        age_share,
        credit_limit
    from eligible_customers
) src
unpivot (
    value for metric in (age_share as AGE_SHARE, credit_limit as CREDIT_LIMIT)
);
