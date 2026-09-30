with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from suppliers s
    where exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.supplier_id is null
        )
)
select
    id,
    country,
    rating
from kept
order by country desc;
