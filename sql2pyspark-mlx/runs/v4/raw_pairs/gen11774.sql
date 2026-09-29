with filtered_stores as (
    select *, round(sqft * 100, 2) as adjusted_sqft
    from stores
    where year(opened_date) = 2025
),
summary as (
    select quarter(opened_date) as opened_quarter, region, concat_ws(', ', sort_array(collect_set(region))) as region_values
    from filtered_stores
    group by quarter(opened_date), region
)
select opened_quarter, region, region_values
from summary
order by opened_quarter, region desc;
