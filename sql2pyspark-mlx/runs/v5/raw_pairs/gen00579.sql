with ranked_stores as (
    select
        id,
        region,
        opened_date,
        rank() over (partition by region order by opened_date) as rn
    from stores
)
select id, region, opened_date
from ranked_stores
where rn <= 3
order by opened_date desc;
