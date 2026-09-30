with kept_keys as (
    select id, region
    from stores
    except all
    select id, region
    from stores
    where sqft < 11798
),
kept_rows as (
    select s.*
    from stores s
    inner join kept_keys k on s.id = k.id and s.region = k.region
)
select
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    case when region rlike '^(North|South)$' then 'GROUP_B' when region rlike '^(East|West)$' then 'CORE' else 'GROUP_A' end as region_zone
from kept_rows
order by opened_date desc
