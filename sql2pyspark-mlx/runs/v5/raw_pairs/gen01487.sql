with valid as (
    -- Basic cleansing and defaults
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        case when promo_code in ('BUNDLE', 'SPRING10') then 'Group A' else 'Other' end as promo_code_group,
        date_sub(starts_on, 59) as starts_on_minus_59d
    from promotions
    where starts_on >= to_date('2023-07-31')
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        last_day(ends_on) as ends_on_month_end,
        case when promo_code in ('VIP', 'BUNDLE') then 'Group A' else 'Other' end as promo_code_group_2
    from valid
    where discount_pct between 0.16 and 0.33
),
cleaned_promotions as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        starts_on_minus_59d,
        ends_on_month_end,
        promo_code_group_2,
        case when discount_pct >= 0.35 then 'High' when discount_pct >= 0.15 then 'Medium' else 'Low' end as discount_pct_tier
    from cleaned
    where ends_on >= date '2023-08-27'
      and promo_code = 'WELCOME5'
),
valid_promotions as (
    -- Filter the raw rows and derive helper columns
    select *, datediff(ends_on_month_end, ends_on) as days_ends_on_to_ends_on_month_end
    from cleaned_promotions
    where starts_on >= date '2023-07-23'
)
select
    id,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    starts_on_minus_59d,
    ends_on_month_end,
    discount_pct_tier,
    days_ends_on_to_ends_on_month_end,
    case when promo_code rlike '^(SPRING10|WELCOME5)$' then 'GROUP_A' else 'OTHER_ZONE' end as promo_code_zone
from valid_promotions;
