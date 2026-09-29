with cleaned as (
    -- Keep recent, valid rows
    select *, lower(product_name) as product_name_lower
    from products
    where launch_date >= date '2023-08-22'
      and brand is not null
)
select category, brand
from cleaned
except distinct
select category, brand
from products
where launch_date <= '2023-11-08';
