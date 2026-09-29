with cleaned as (
    -- Basic cleansing and defaults
    select *, add_months(opened_date, 3) as opened_date_plus_months
    from stores
    where region is not null
      and region is not distinct from 'North'
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        city,
        region,
        sqft,
        first_value(sqft, true) over (partition by region order by opened_date, id rows between unbounded preceding and current row) as first_known_sqft,
        row_number() over (partition by city order by opened_date desc, id) as event_seq,
        row_number() over (partition by region, city order by sqft desc, id) as row_seq,
        last_value(sqft) ignore nulls over (partition by city, region order by sqft, id rows between unbounded preceding and current row) as last_known_sqft,
        last_value(region) ignore nulls over (partition by city order by opened_date, id rows between unbounded preceding and current row) as last_known_region
    from cleaned
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        region,
        sqft,
        first_known_sqft,
        event_seq,
        last_known_region
    from metrics
    qualify row_number() over (partition by region, city order by event_seq, id) = 1
),
latest_match as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.region,
        s.first_known_sqft,
        s.event_seq,
        top1.inventory_last_restock,
        top1.quantity
    from latest s
    left join lateral (
            select c.last_restock as inventory_last_restock, c.quantity
            from inventory c
            where c.store_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
matched_keys as (
    -- Set difference on keys
    select id, region
    from latest_match
    except
    select id, region
    from stores
    where region in ('West', 'East')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest_match s
    inner join matched_keys k on s.id = k.id and s.region = k.region
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from reconciled s
    where exists (
            select 1
            from inventory x
            where x.store_id = s.id
              and x.store_id in (1, 2, 5)
        )
),
grouped as (
    -- Aggregate with conditional metrics
    select
        date_trunc('week', inventory_last_restock) as week_start,
        sort_array(collect_set(region)) as region_list,
        percentile_approx(first_known_sqft, 0.5) as first_known_sqft_p50,
        approx_percentile(quantity, 0.9) as quantity_p90,
        sum(case when region = 'Central' then first_known_sqft else 0 end) as central_first_known_sqft,
        count(case when region = 'Central' then 1 end) as central_count
    from eligible
    group by week_start
    having first_known_sqft_p50 > 0
)
select
    week_start,
    region_list,
    quantity_p90,
    central_first_known_sqft
from grouped;
