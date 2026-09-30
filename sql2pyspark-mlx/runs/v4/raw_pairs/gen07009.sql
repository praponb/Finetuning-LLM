with with_windows as (
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        last_value(sqft, true) over w_ordered as last_known_sqft,
        avg(sqft) over w_part as sqft_avg_5,
        row_number() over (partition by region order by sqft desc, id) as seq_no
    from stores
    window w_ordered as (partition by region order by sqft, id rows between unbounded preceding and current row), w_part as (order by sqft, id rows between 5 preceding and current row)
)
select
    id,
    city,
    opened_date,
    last_known_sqft,
    sqft_avg_5,
    seq_no,
    case when city rlike '^(Bangkok|Tokyo)$' then 'GROUP_B' when city rlike '^(Chiang Mai|New York)$' then 'CORE' else 'GROUP_A' end as city_zone
from with_windows
order by opened_date desc nulls last;
