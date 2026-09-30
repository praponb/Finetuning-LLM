with kept_keys as (
    -- Set intersection on keys
    select city
    from suppliers
    intersect all
    select city
    from suppliers
    where rating >= 3.8
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from suppliers s
    inner join kept_keys k on s.city = k.city
)
select
    supplier_name,
    city,
    rating
from kept_rows
order by rating;
