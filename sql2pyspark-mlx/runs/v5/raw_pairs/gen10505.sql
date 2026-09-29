with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        k,
        value
    from carts
    lateral view outer explode(attributes) t as k, value
),
joined as (
    -- Attach customers attributes
    select
        e.*,
        d.segment,
        d.city,
        d.name
    from exploded e
    join customers d
        on e.customer_id = d.id
),
joined_carts as (
    -- Attach customers attributes
    select
        s.*,
        b.email,
        b.age
    from joined s
    left join customers b
        on s.customer_id = b.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        customer_id,
        k,
        value,
        city,
        age,
        cume_dist() over (partition by city order by age nulls first) as age_cume_dist
    from joined_carts
    where city = 'Bangkok'
),
with_dims as (
    -- Attach customers attributes
    select
        src.*,
        c.signup_date,
        c.city as customer_city,
        c.credit_limit
    from metrics src
    left join customers c
        on src.customer_id = c.id
),
scored as (
    -- Running and moving aggregates
    select
        customer_id,
        k,
        round(age / sum(age) over (partition by customer_id), 4) as age_share,
        dense_rank() over (partition by k, customer_city order by age desc nulls first) as age_dense_rank,
        max(credit_limit) over (partition by customer_id) as customer_id_max_credit_limit,
        stddev_samp(age) over (partition by city) as city_stddev_samp_age
    from with_dims
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (order by age_share desc nulls first) as age_share_dense_rank,
        avg(customer_id_max_credit_limit) over w_part as k_avg_customer_id_max_credit_limit,
        round(age_dense_rank / sum(age_dense_rank) over (partition by customer_id), 4) as age_dense_rank_share,
        dense_rank() over (order by city_stddev_samp_age desc) as city_stddev_samp_age_dense_rank,
        round(customer_id_max_credit_limit / sum(customer_id_max_credit_limit) over (partition by customer_id), 4) as customer_id_max_credit_limit_share
    from scored
    window w_part as (partition by k)
)
-- Join a dimension and aggregate
select
    dim.country,
    dim.segment,
    e.k,
    count(*) as cnt,
    max(e.age_dense_rank_share) as max_age_dense_rank_share,
    grouping_id() as gid
from with_windows e
left join customers dim on e.customer_id = dim.id
group by rollup (dim.country, dim.segment, e.k)
order by k
