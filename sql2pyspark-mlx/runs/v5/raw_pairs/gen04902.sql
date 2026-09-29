with flattened as (
    select
        id,
        host,
        level,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) li as attr_key, v
),
unpacked as (
    select
        id,
        host,
        level,
        attr_key,
        v,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 8) != 0 as int) as flag_bit8
    from flattened
),
metrics as (
    select
        id,
        level,
        v,
        status_code_high,
        flag_bit8,
        dense_rank() over (partition by level order by flag_bit8 desc nulls first) as flag_bit8_dense_rank,
        dense_rank() over w_hist as status_code_high_dense_rank,
        avg(status_code_high) over (partition by attr_key) as attr_key_avg_status_code_high,
        percent_rank() over (order by flag_bit8 nulls first) as flag_bit8_percent_rank,
        rank() over (partition by host order by flag_bit8 desc nulls first) as flag_bit8_rank
    from unpacked
    where host in ('h3', 'h4')
    window w_hist as (partition by attr_key order by status_code_high nulls first)
),
base as (
    select *, round(status_code_high / flag_bit8_dense_rank, 2) as status_code_high_per_flag_bit8_dense_rank
    from metrics
    where status_code_high_dense_rank = 6
),
cleaned as (
    select
        id,
        level,
        v,
        flag_bit8,
        flag_bit8_dense_rank,
        status_code_high_dense_rank,
        flag_bit8_percent_rank,
        flag_bit8_rank,
        status_code_high_per_flag_bit8_dense_rank,
        length(level) as level_length,
        trim(level) as clean_level
    from base
    where status_code_high_dense_rank is not null
),
cleaned_app_logs as (
    select *, flag_bit8_dense_rank / nullif(status_code_high_per_flag_bit8_dense_rank, 0) as flag_bit8_dense_rank_per_status_code_high_per_flag_bit8_dense_rank
    from cleaned
    where flag_bit8 = 1
),
with_windows as (
    select
        *,
        rank() over (partition by level order by flag_bit8_rank desc nulls last) as flag_bit8_rank_rank,
        avg(status_code_high_dense_rank) over (partition by level) as level_avg_status_code_high_dense_rank,
        sum(status_code_high_per_flag_bit8_dense_rank) over (partition by level) as level_sum_status_code_high_per_flag_bit8_dense_rank
    from cleaned_app_logs
)
select
    id,
    flag_bit8,
    status_code_high_dense_rank,
    flag_bit8_percent_rank,
    flag_bit8_rank,
    level_length,
    flag_bit8_dense_rank_per_status_code_high_per_flag_bit8_dense_rank,
    level_avg_status_code_high_dense_rank,
    level_sum_status_code_high_per_flag_bit8_dense_rank
from with_windows
order by level_length;
