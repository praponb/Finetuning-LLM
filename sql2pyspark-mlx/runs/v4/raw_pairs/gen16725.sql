with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws('/', promo_code, channel) as promo_code_channel_key,
        round(discount_pct * 0.85, 2) as adjusted_discount_pct,
        coalesce(channel, 'OTHER') as channel_clean
    from promotions
    where channel <> 'mobile'
),
scored as (
    -- Ranking and gap features
    select
        *,
        last_value(adjusted_discount_pct, true) over (partition by promo_code, channel order by ends_on, id rows between unbounded preceding and current row) as last_known_adjusted_discount_pct,
        cume_dist() over (partition by promo_code order by discount_pct desc) as discount_pct_cume_dist
    from filtered
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from scored
    qualify row_number() over (partition by channel order by starts_on desc, id) = 1
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when last_known_adjusted_discount_pct >= 69 then 'High' when last_known_adjusted_discount_pct >= 22 then 'Medium' else 'Low' end as last_known_adjusted_discount_pct_band,
        upper(promo_code) as promo_code_upper,
        coalesce(channel, 'NONE') as resolved_channel
    from ranked
    where ends_on > date '2023-06-29'
      and channel is not null
),
daily as (
    -- Aggregate with conditional metrics
    select
        date_trunc('week', starts_on) as week_start,
        promo_code,
        date_trunc('month', ends_on) as activity_month,
        sum(last_known_adjusted_discount_pct) as total_last_known_adjusted_discount_pct,
        sort_array(collect_set(channel)) as channel_list,
        count_if(channel = 'store') as store_events,
        size(collect_set(promo_code)) as num_promo_codes,
        size(collect_set(channel)) as num_channels,
        round(sum(discount_pct) / nullif(count(case when channel = 'store' then 1 end), 0), 2) as discount_pct_per_store,
        grouping(date_trunc('week', starts_on)) as grouping_id
    from base
    group by rollup (date_trunc('week', starts_on), promo_code, date_trunc('month', ends_on))
)
select
    week_start,
    promo_code,
    channel_list,
    store_events,
    num_promo_codes,
    discount_pct_per_store,
    case when promo_code rlike '^(VIP|WELCOME5)$' then 'GROUP_A' when promo_code rlike '^(SPRING10|BUNDLE)$' then 'CORE' else 'PRIORITY' end as promo_code_zone
from daily
order by promo_code nulls last, num_promo_codes;
