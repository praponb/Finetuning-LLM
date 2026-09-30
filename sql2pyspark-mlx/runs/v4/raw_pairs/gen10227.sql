with filtered as (
    -- Keep recent, valid rows
    select *, date_add(starts_on, 42) as starts_on_plus_42d
    from promotions
    where discount_pct >= 0.28
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        round(discount_pct / sum(discount_pct) over (partition by promo_code), 4) as discount_pct_share,
        ntile(5) over w_grp as discount_pct_bucket
    from filtered
    window w_grp as (partition by channel order by discount_pct desc, id asc)
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        sum(discount_pct) over (partition by channel order by starts_on asc, id asc rows between unbounded preceding and current row) as cumulative_discount_pct,
        first_value(discount_pct_bucket) ignore nulls over (partition by promo_code order by discount_pct asc, id asc rows between unbounded preceding and current row) as first_known_discount_pct_bucket
    from metrics
),
with_window as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from scored e
    left join promotions pr
        on coalesce(e.channel, 'web') = pr.channel
       and e.ends_on >= pr.starts_on
       and e.ends_on <= date_add(pr.ends_on, 10)
)
select
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    starts_on_plus_42d,
    discount_pct_share,
    discount_pct_bucket,
    case when channel rlike '^(mobile|web)$' then 'GROUP_B' when channel rlike '^(store|partner)$' then 'PRIORITY' else 'OTHER_ZONE' end as channel_zone
from with_window;
