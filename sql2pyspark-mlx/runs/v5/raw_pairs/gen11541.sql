with deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by segment order by credit_limit nulls first, id) as dedup_rank
    from customers
    qualify dedup_rank = 1
)
select
    id,
    name,
    email,
    country,
    segment,
    signup_date,
    is_active,
    credit_limit,
    dedup_rank
from deduped
order by segment
