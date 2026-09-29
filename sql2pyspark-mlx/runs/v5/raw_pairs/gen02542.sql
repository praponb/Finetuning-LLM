with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws(',', country, supplier_name) as country_supplier_name_key,
        floor(rating) as rating_whole,
        coalesce(city, 'DIRECT') as resolved_city
    from suppliers
    where country in ('US', 'SG', 'TH')
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from filtered s
    where exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.cost between 250 and 600
        )
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        supplier_name,
        country,
        country_supplier_name_key,
        rating_whole,
        resolved_city,
        sum(rating) over (partition by country) as country_sum_rating,
        avg(rating_whole) over (partition by country order by rating, id rows between 5 preceding and current row) as rating_whole_avg_5
    from eligible
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        case when country = 'SG' then 1 else 0 end as is_sg,
        upper(country) as country_upper
    from enriched
    where rating_whole >= 27
)
-- Multi-level aggregation
select
    country,
    case when rating_whole >= 81 then 'PREMIUM' else 'STANDARD' end as rating_whole_tier,
    sum(rating_whole_avg_5) filter (where country = 'US') as us_total_rating_whole_avg_5,
    count_if(country = 'TH') as th_events,
    count(*) filter (where country = 'DE') as de_rows,
    count(*) as row_count
from valid
group by country, case when rating_whole >= 81 then 'PREMIUM' else 'STANDARD' end
