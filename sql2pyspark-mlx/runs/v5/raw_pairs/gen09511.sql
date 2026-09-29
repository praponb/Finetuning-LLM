with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        promo_code,
        starts_on,
        date_format(starts_on, 'yyyy-MM') as starts_on_label,
        coalesce(channel, 'NONE') as channel_filled
    from promotions
    where starts_on >= date '2023-06-13'
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        promo_code,
        starts_on_label
    from (
        select *, row_number() over (partition by promo_code order by starts_on desc, id) as latest_rank
        from cleaned
    ) t
    where latest_rank = 1
),
kept_keys as (
    -- Set difference on keys
    select promo_code, id
    from latest
    except
    select promo_code, id
    from promotions
    where ends_on >= '2025-10-23'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    join kept_keys k on s.promo_code = k.promo_code and s.id = k.id
),
cleaned_promotions as (
    -- Basic cleansing and defaults
    select
        *,
        trim(promo_code) as clean_promo_code,
        case when promo_code in ('VIP', 'SPRING10') then 'Core' else 'Other' end as promo_code_group
    from reconciled
    where promo_code = 'SPRING10'
)
select clean_promo_code
from cleaned_promotions
