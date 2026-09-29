with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        val
    from price_tiers
    lateral view outer explode(tier_limits) t as val
),
filtered as (
    -- Keep recent, valid rows
    select *, cast(val as string) as val_str
    from lines
    where plan <> 'enterprise'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, cast(val as double) as val_str_2
    from filtered
    where plan in ('enterprise', 'starter')
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        plan,
        val,
        val_str,
        val_str_2,
        ceil(val) as val_whole
    from cleaned
    where val between 21 and 40
)
select
    val_str,
    val_str_2,
    val_whole
from valid
