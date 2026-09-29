with flattened_attrs as (
    select
        id,
        log_ts,
        level,
        status_code,
        payload_json,
        context,
        cast((status_code & 8) != 0 as int) as flag_bit8,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec
    from app_logs
)
select
    *,
    row_number() over (partition by level order by retry_count desc, id) as seq_no,
    sum(flag_bit8) over (partition by level order by retry_count, id rows between unbounded preceding and current row) as running_flag_bit8,
    percent_rank() over (partition by level order by flag_bit8 desc) as flag_bit8_percent_rank,
    first_value(flag_bit8, true) over (partition by level order by duration_sec, id rows between unbounded preceding and current row) as first_known_flag_bit8
from flattened_attrs;
