with ranked as (
    -- QUALIFY-style window filter
    select
        id,
        promo_code,
        ends_on
    from promotions
    qualify rank() over (partition by promo_code order by ends_on) = 1
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by promo_code order by ends_on desc, id) as latest_rank
    from ranked
    qualify latest_rank = 1
),
filtered as (
    -- Basic cleansing and defaults
    select *, date_sub(ends_on, 39) as ends_on_minus_39d
    from latest
    where ends_on <= '2023-05-23'
),
deduped as (
    -- QUALIFY-style window filter
    select id, promo_code
    from filtered
    qualify rank() over (partition by promo_code order by ends_on desc) = 1
)
select
    id,
    promo_code,
    case when promo_code rlike '^(VIP|BUNDLE)$' then 'GROUP_B' when promo_code rlike '^(SPRING10|WELCOME5)$' then 'CORE' else 'PRIORITY' end as promo_code_zone
from deduped
order by promo_code_zone nulls first;
