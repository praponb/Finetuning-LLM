with filtered as (
    select
        *,
        length(supplier_name) as supplier_name_length,
        cast(rating as int) as rating_int,
        coalesce(city, 'N/A') as resolved_city
    from suppliers
    where rating >= 2.4
),
with_windows as (
    select
        id,
        rating_int,
        row_number() over (partition by country order by rating_int, id) as event_seq,
        sum(supplier_name_length) over (order by supplier_name_length, id rows between unbounded preceding and current row) as running_supplier_name_length,
        ntile(3) over (partition by city order by rating desc, id) as rating_bucket,
        lead(rating, 1, 0) over (partition by country order by rating, id) as next_rating,
        lead(supplier_name_length, 1) over (partition by city order by rating, id) as next_supplier_name_length
    from filtered
),
with_windows_suppliers as (
    select
        *,
        min(rating_bucket) over (order by next_supplier_name_length, id rows between unbounded preceding and current row) as running_min_rating_bucket,
        max(rating_bucket) over (order by next_supplier_name_length, id rows between unbounded preceding and current row) as running_max_rating_bucket,
        count(rating_bucket) over (order by next_supplier_name_length, id rows between unbounded preceding and current row) as running_count_rating_bucket
    from with_windows
),
with_windows_suppliers_2 as (
    select
        id,
        rating_int,
        running_supplier_name_length,
        rating_bucket,
        next_rating,
        running_min_rating_bucket,
        running_count_rating_bucket,
        avg(running_count_rating_bucket) over (order by event_seq, id rows between unbounded preceding and current row) as running_avg_running_count_rating_bucket,
        count(running_count_rating_bucket) over (order by event_seq, id rows between unbounded preceding and current row) as running_count_running_count_rating_bucket,
        max(running_count_rating_bucket) over (order by event_seq, id rows between unbounded preceding and current row) as running_max_running_count_rating_bucket,
        min(running_count_rating_bucket) over (order by event_seq, id rows between unbounded preceding and current row) as running_min_running_count_rating_bucket,
        cume_dist() over (order by running_max_rating_bucket desc nulls last) as running_max_rating_bucket_cume_dist
    from with_windows_suppliers
),
scored as (
    select
        id,
        rating_int,
        running_supplier_name_length,
        rating_bucket,
        running_min_rating_bucket,
        running_avg_running_count_rating_bucket,
        running_max_running_count_rating_bucket,
        running_min_running_count_rating_bucket,
        running_max_rating_bucket_cume_dist,
        row_number() over (order by running_supplier_name_length desc, id) as seq_no,
        avg(running_supplier_name_length) over (order by running_count_rating_bucket, id rows between 2 preceding and current row) as running_supplier_name_length_avg_2
    from with_windows_suppliers_2
)
select
    id,
    rating_int,
    running_supplier_name_length,
    rating_bucket,
    running_avg_running_count_rating_bucket,
    running_max_running_count_rating_bucket,
    running_min_running_count_rating_bucket,
    running_max_rating_bucket_cume_dist,
    seq_no
from scored
order by rating_int nulls first;
