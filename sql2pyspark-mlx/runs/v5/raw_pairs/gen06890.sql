with enriched as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(city, true) over (partition by country order by credit_limit, id rows between unbounded preceding and current row) as last_known_city,
        first_value(city) ignore nulls over (partition by segment order by credit_limit, id rows between unbounded preceding and current row) as first_known_city,
        sum(age) over (partition by country order by signup_date, id rows between unbounded preceding and current row) as running_age,
        first_value(segment, true) over (order by signup_date, id rows between unbounded preceding and current row) as first_known_segment
    from customers
    where country = 'US'
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from subscriptions c
            where c.customer_id = s.id
              and c.end_date >= s.signup_date
        )
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by segment order by running_age nulls first, id) as latest_rank
        from kept
    ) t
    where latest_rank = 1
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        max(running_age) over w_running as running_max_running_age,
        avg(running_age) over w_running as running_avg_running_age,
        min(running_age) over w_running as running_min_running_age,
        count(running_age) over w_running as running_count_running_age,
        avg(age) over (partition by segment order by credit_limit, id rows between 3 preceding and current row) as age_avg_3
    from deduped
    window w_running as (partition by city order by running_age, id rows between unbounded preceding and current row)
)
select
    id,
    email,
    country,
    city,
    age,
    first_known_segment,
    latest_rank,
    running_max_running_age,
    running_avg_running_age
from with_windows
order by running_avg_running_age, age desc
