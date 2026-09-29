with valid as (
    select
        *,
        cast(sqft as string) as sqft_str,
        concat_ws('-', store_name, region) as store_name_region_key
    from stores
    where region is not null
),
ranked as (
    select *
    from valid
    qualify rank() over (partition by city order by sqft desc nulls last) = 1
),
with_latest as (
    select
        s.*,
        x.discount,
        x.order_amount
    from ranked s
    left join lateral (
            select c.discount, c.amount as order_amount
            from orders c
            where c.store_id = s.id
              and c.order_date <= s.opened_date
            order by c.order_date desc, c.id desc
            limit 1
        ) x on true
),
filtered as (
    select
        id,
        store_name,
        city,
        opened_date,
        sqft_str,
        store_name_region_key,
        order_amount,
        concat(store_name, ' ', city) as store_name_city,
        upper(city) as city_upper,
        coalesce(region, 'N/A') as resolved_region
    from with_latest
    where opened_date >= date '2023-09-03'
      and region is not null
),
deduped as (
    select *
    from filtered
    qualify row_number() over (partition by city order by order_amount desc, id) = 1
),
remaining_keys as (
    select id, city
    from deduped
    except
    select id, city
    from stores
    where store_name like '%Riverside%'
),
verified as (
    select s.*
    from deduped s
    inner join remaining_keys k on s.id = k.id and s.city = k.city
)
select
    city,
    sqft_str,
    store_name_region_key,
    store_name_city,
    city_upper,
    case when city rlike '^(Singapore|Chiang Mai)$' then 'GROUP_B' else 'GROUP_A' end as city_zone
from verified;
