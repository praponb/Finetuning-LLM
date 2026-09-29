with kept_keys as (
    -- Set intersection on keys
    select id
    from promotions
    intersect
    select id
    from promotions
    where promo_code = 'WELCOME5'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from promotions s
    inner join kept_keys k on s.id = k.id
)
-- Aggregate with conditional metrics
select
    date_trunc('month', ends_on) as activity_month,
    promo_code,
    count(case when channel = 'store' then 1 end) as store_count,
    sort_array(collect_set(promo_code)) as promo_code_list,
    count(case when promo_code = 'VIP' then 1 end) as vip_count,
    dense_rank() over (partition by date_trunc('month', ends_on) order by count(case when channel = 'store' then 1 end) desc) as store_count_dense_rank
from kept_rows
group by 1, 2
order by store_count desc;
