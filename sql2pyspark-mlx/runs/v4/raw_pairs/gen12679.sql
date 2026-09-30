with eligible as (
    select s.*
    from stores s
    where exists (
            select 1
            from store_sales x
            where x.store_id = s.id
              and x.sale_date <= s.opened_date
        )
)
select
    date_trunc('year', opened_date) as year_start,
    stddev_samp(sqft) as sqft_stddev,
    approx_percentile(sqft, 0.95) as sqft_p95,
    count(case when city = 'Bangkok' then 1 end) as bangkok_count
from eligible
group by 1;
