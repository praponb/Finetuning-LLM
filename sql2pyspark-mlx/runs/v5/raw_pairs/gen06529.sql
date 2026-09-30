with valid as (
    -- Keep recent, valid rows
    select *, length(product_name) product_name_length
    from products
    where supplier_id is not null
      and cost < 370
)
-- Semi-join via a correlated subquery
select s.*
from valid s
where exists (
        select 1
        from inventory ch
        where ch.product_id = s.id
          and ch.last_restock > '2024-11-01'
    )
