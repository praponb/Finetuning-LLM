with base as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        sale_date,
        units,
        revenue,
        revenue + units as revenue_per_unit
    from store_sales
    where revenue < 1500
)
-- Attach stores attributes
select
    src.*,
    s.store_name,
    s.sqft
from base src
left join stores s
    on src.store_id = s.id;
