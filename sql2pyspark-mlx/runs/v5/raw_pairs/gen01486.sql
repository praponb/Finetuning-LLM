with enriched as (
    -- Attach products attributes
    select
        s.*,
        d.cost,
        d.brand
    from order_items s
    left join products d
        on s.product_id <=> d.id
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        unit_price,
        brand,
        row_number() over (partition by brand order by cost desc, id) as seq_no,
        sum(unit_price) over (partition by brand, order_id order by unit_price, id rows between unbounded preceding and current row) as running_unit_price,
        lag(cost, 2, 0) over (order by quantity, id) as prev_cost
    from enriched
    where cost >= 300
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        unit_price,
        brand,
        seq_no,
        running_unit_price,
        prev_cost,
        trim(brand) as clean_brand,
        ceil(unit_price) as unit_price_whole,
        coalesce(brand, 'NONE') as brand_clean
    from metrics
    where brand is not null
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        case when brand = 'Initech' then 1 else 0 end as is_initech,
        case when running_unit_price >= 1623 then 'High' when running_unit_price >= 659 then 'Medium' else 'Low' end as running_unit_price_band
    from valid
    where brand is not null
      and brand is distinct from 'Globex'
)
select
    id,
    brand,
    running_unit_price,
    clean_brand,
    unit_price_whole,
    brand_clean,
    is_initech,
    running_unit_price_band,
    case when brand rlike '^(Umbrella|Stark)$' then 'CORE' when brand rlike '^(Acme|Globex)$' then 'GROUP_B' else 'PRIORITY' end as brand_zone
from filtered
order by id
