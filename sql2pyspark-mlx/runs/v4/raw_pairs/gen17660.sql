with not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from stores s
    where not exists (
            select 1
            from inventory c
            where c.store_id = s.id
              and c.last_restock >= s.opened_date
        )
),
scored as (
    -- Ranking and gap features
    select
        *,
        round(sqft / sum(sqft) over (partition by city), 4) sqft_share,
        sum(sqft) over w_recent running_sqft
    from not_flagged
    window w_recent as (partition by city, region order by sqft, id rows between unbounded preceding and current row)
),
not_flagged_stores as (
    -- Semi-join via a correlated subquery
    select s.*
    from scored s
    where exists (
            select 1
            from inventory x
            where x.store_id = s.id
              and x.last_restock <= s.opened_date
        )
),
latest_match as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select s.*, top1.last_restock
    from not_flagged_stores s
    join lateral (
            select c.last_restock
            from inventory c
            where c.store_id = s.id
              and c.last_restock <= s.opened_date
            order by c.last_restock desc, c.id desc
            limit 1
        ) top1 on true
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        city,
        sqft,
        running_sqft,
        round(running_sqft * 0.85, 2) running_sqft_scaled
    from latest_match
    where region is not null
      and upper(trim(store_name)) = 'AIRPORT KIOSK'
),
grouped as (
    -- Multi-level aggregation
    select
        city,
        sum(case when city = 'Singapore' then running_sqft else 0 end) as singapore_running_sqft,
        count_if(city = 'Berlin') as berlin_events,
        round(avg(running_sqft), 2) as avg_running_sqft,
        count(case when city = 'Berlin' then 1 end) as berlin_count,
        avg(running_sqft_scaled) filter (where city = 'Singapore') as singapore_avg_running_sqft_scaled
    from cleaned
    group by city
    having singapore_avg_running_sqft_scaled >= 0
)
select
    city,
    singapore_running_sqft,
    berlin_events,
    avg_running_sqft,
    singapore_avg_running_sqft_scaled
from grouped
order by berlin_events desc;
