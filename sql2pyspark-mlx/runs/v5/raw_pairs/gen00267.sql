with kept_keys as (
    -- Set intersection on keys
    select promo_code, id
    from promotions
    intersect all
    select promo_code, id
    from promotions
    where channel in ('store', 'web')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from promotions s
    inner join kept_keys k on s.promo_code = k.promo_code and s.id = k.id
)
select
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct
from reconciled
order by channel desc nulls first
