with with_latest as (
    select s.*, latest.cost
    from suppliers s
    left join lateral (
            select c.cost
            from products c
            where c.supplier_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) latest on true
),
latest as (
    select *
    from with_latest
    qualify row_number() over (partition by country order by rating desc, id) = 1
),
remaining_keys as (
    select id
    from latest
    except
    select id
    from suppliers
    where country in ('JP', 'SG')
),
kept_rows as (
    select s.*
    from latest s
    join remaining_keys k on s.id = k.id
),
metrics as (
    select
        *,
        percent_rank() over w_recent cost_percent_rank,
        round(rating / sum(rating) over (partition by country), 4) rating_share,
        count(cost) over (partition by country) country_count_cost,
        round(cost / sum(cost) over (partition by country), 4) cost_share
    from kept_rows
    where city = 'London'
    window w_recent as (partition by country, city order by cost desc)
),
matched_keys as (
    select id, country
    from metrics
    except
    select id, country
    from suppliers
    where rating > 3.9
),
kept_rows_suppliers as (
    select s.*
    from metrics s
    join matched_keys k on s.id = k.id and s.country = k.country
),
metrics_suppliers as (
    select
        supplier_name,
        country,
        city,
        rating,
        country_count_cost,
        rank() over w_ordered cost_percent_rank_rank,
        percent_rank() over (order by rating) rating_percent_rank,
        round(rating / sum(rating) over (partition by city), 4) rating_share_2,
        dense_rank() over (order by rating nulls first) rating_dense_rank
    from kept_rows_suppliers
    window w_ordered as (order by cost_percent_rank desc)
),
remaining_keys_2 as (
    select city, country
    from metrics_suppliers
    except all
    select city, country
    from suppliers
    where city = 'Bangkok'
),
reconciled as (
    select s.*
    from metrics_suppliers s
    join remaining_keys_2 k on s.city = k.city and s.country = k.country
)
select
    country,
    count(case when country = 'US' then 1 end) as us_count,
    percentile_approx(cost_percent_rank_rank, array(0.25, 0.5, 0.75)) as cost_percent_rank_rank_quartiles,
    count(distinct city) as distinct_cities,
    count(distinct country) as distinct_countries,
    count(case when city = 'Berlin' then 1 end) as berlin_count
from reconciled
group by country
order by distinct_countries;
