with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, length(country) as country_length
    from customers
    where signup_date >= date '2023-08-05'
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from filtered s
    where exists (
            select 1
            from accounts c
            where c.customer_id = s.id
              and c.opened_date between '2023-06-01' and '2025-02-01'
        )
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by segment order by credit_limit desc, id) as dedup_rank
    from kept
    qualify dedup_rank = 1
)
-- Aggregate with conditional metrics
select
    country,
    bit_or(age) as age_bit_or,
    bit_and(age) as age_bit_and
from latest
group by country
having bit_and(age) > 1
order by age_bit_and desc nulls last
