with lines as (
    -- Unnest the array into one row per element
    select id, explode(coupon_codes) as element
    from carts
),
matched_keys as (
    -- Set difference on keys
    select id
    from lines
    except all
    select id
    from carts
    where array_contains(coupon_codes, 'WELCOME5')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from lines s
    inner join matched_keys k on s.id = k.id
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        case when element = 'WELCOME5' then 1 else 0 end as is_welcome5,
        coalesce(element, 'UNKNOWN') as element_filled
    from verified
    where element = 'VIP'
      and element is not distinct from 'BUNDLE'
),
cleaned_carts as (
    -- Basic cleansing and defaults
    select *, case when is_welcome5 >= 63 then 'High' when is_welcome5 >= 20 then 'Medium' else 'Low' end as is_welcome5_band
    from cleaned
    where element is distinct from 'BUNDLE'
),
filtered as (
    -- Basic cleansing and defaults
    select *, ceil(is_welcome5) as is_welcome5_whole
    from cleaned_carts
    where element is not null
)
-- Aggregate with conditional metrics
select
    element,
    case when is_welcome5_whole >= 90 then 'PREMIUM' else 'STANDARD' end as is_welcome5_whole_tier,
    approx_percentile(is_welcome5_whole, 0.9) as is_welcome5_whole_p90,
    approx_percentile(is_welcome5, 0.95) as is_welcome5_p95,
    avg(is_welcome5) as avg_is_welcome5,
    max(is_welcome5) filter (where element = 'SPRING10') as spring10_max_is_welcome5,
    count(case when element = 'BUNDLE' then 1 end) as bundle_count
from filtered
group by element, case when is_welcome5_whole >= 90 then 'PREMIUM' else 'STANDARD' end
order by element;
