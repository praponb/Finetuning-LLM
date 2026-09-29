with not_flagged as (
    select s.*
    from stores s
    where not exists (
            select 1
            from store_sales c
            where c.store_id = s.id
              and c.sale_date < s.opened_date
        )
),
scored as (
    select
        *,
        avg(sqft) over (partition by city order by sqft, id rows between 3 preceding and current row) as sqft_avg_3,
        count(sqft) over (partition by city) as city_count_sqft
    from not_flagged
    where opened_date >= '2024-05-01'
),
latest as (
    select
        id,
        store_name,
        sqft,
        sqft_avg_3,
        city_count_sqft
    from (
        select *, row_number() over (partition by city order by sqft desc nulls first, id) as dedup_rank
        from scored
    ) t
    where dedup_rank = 1
),
kept as (
    select s.*
    from latest s
    where exists (
            select 1
            from orders c
            where c.store_id = s.id
              and c.amount < 1250
        )
),
filtered as (
    select
        id,
        store_name,
        sqft,
        sqft_avg_3,
        upper(trim(store_name)) as store_name_normalized
    from kept
    where sqft_avg_3 is not null
)
select
    id,
    store_name,
    sqft,
    store_name_normalized
from filtered
order by id desc nulls last;
