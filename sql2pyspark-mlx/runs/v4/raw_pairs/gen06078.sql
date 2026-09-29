with top_suppliers as (
    select *, row_number() over (partition by city order by rating desc, id) as pos
    from suppliers
)
select *
from top_suppliers
where pos <= 2;
