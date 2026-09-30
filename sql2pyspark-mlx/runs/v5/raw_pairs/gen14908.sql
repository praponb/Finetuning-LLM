with ranked as (
    select
        id,
        category,
        cost,
        launch_date,
        dense_rank() over (partition by category order by cost desc) as rn
    from products
)
select *
from ranked
where rn <= 5;
