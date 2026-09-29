with base as (
    -- Basic cleansing and defaults
    select
        *,
        datediff(current_date(), ends_on) as days_since_ends_on,
        coalesce(channel, 'OTHER') as resolved_channel
    from promotions
    where ends_on >= to_date('2023-09-05')
      and channel is not null
)
-- QUALIFY-style window filter
select
    id,
    channel,
    promo_code,
    ends_on,
    resolved_channel
from (
    select *, row_number() over (partition by promo_code order by starts_on desc, id) as row_num
    from base
) x
where row_num = 1
