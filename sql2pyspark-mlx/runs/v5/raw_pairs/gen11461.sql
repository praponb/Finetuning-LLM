with with_windows as (
    -- Ranking and gap features
    select
        *,
        lead(age, 2) over w_part as next_age,
        row_number() over (partition by city order by credit_limit desc, id) as event_seq
    from customers
    where credit_limit > 11800
    window w_part as (partition by country order by credit_limit, id)
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        lag(credit_limit) over w_part as prev_credit_limit,
        lead(credit_limit) over w_part as next_credit_limit,
        lag(next_age) over w_part as prev_next_age,
        lead(next_age) over w_part as next_next_age,
        row_number() over w_part as event_seq_2
    from with_windows
    window w_part as (partition by city order by signup_date, id)
),
deduped as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by segment, city order by credit_limit nulls first) as dedup_rank
    from enriched
    qualify dedup_rank = 1
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from deduped s
    where exists (
            select 1
            from accounts ch
            where ch.customer_id = s.id
              and ch.opened_date >= s.signup_date
        )
)
select
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    credit_limit,
    age
from kept
order by segment desc
