with filtered as (
    -- Keep recent, valid rows
    select
        *,
        upper(trim(store_name)) as store_name_normalized,
        coalesce(region, 'N/A') as region_filled
    from stores
    where upper(store_name) like '%IVE%'
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        last_value(region, true) over (partition by region order by sqft, id rows between unbounded preceding and current row) as last_known_region,
        lag(sqft, 1, 0) over (partition by region order by sqft, id) as prev_sqft,
        lag(sqft, 1) over (partition by region, city order by sqft, id) as prev_sqft_2,
        row_number() over (partition by city order by sqft desc, id) as seq_no
    from filtered
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        sqft - seq_no as sqft_minus_seq_no,
        date_format(opened_date, 'yyyy-MM-dd') as opened_date_label
    from with_windows
    where region is not null
      and city <> 'Singapore'
),
totals as (
    -- Multi-level aggregation
    select
        case when sqft_minus_seq_no >= 79 then 'HIGH' else 'LOW' end as sqft_minus_seq_no_tier,
        count(*) as row_count,
        count(*) filter (where city = 'Tokyo') as tokyo_rows,
        sum(case when region = 'Central' then sqft else 0 end) as central_sqft,
        bit_and(sqft) as sqft_bit_and,
        grouping_id() as grouping_id
    from base
    group by cube (case when sqft_minus_seq_no >= 79 then 'HIGH' else 'LOW' end)
)
select
    row_count,
    tokyo_rows,
    central_sqft,
    sqft_bit_and
from totals
order by central_sqft nulls last, sqft_bit_and desc;
