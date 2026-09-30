with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        case when is_flagged then 'Yes' else 'No' end as is_flagged_label,
        substring(txn_type, 1, 3) as txn_type_prefix,
        coalesce(merchant, 'NONE') as resolved_merchant
    from transactions
    where txn_time >= date '2023-08-11'
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by merchant, account_id order by txn_time desc nulls first, id) as row_num
        from cleaned
    ) x
    where row_num = 1
),
base as (
    -- Keep recent, valid rows
    select
        id,
        account_id,
        amount,
        merchant,
        is_flagged,
        txn_type_prefix,
        resolved_merchant,
        row_num,
        concat_ws('/', txn_type, merchant) as txn_type_merchant_key
    from latest
    where txn_time >= to_timestamp('2023-08-03 22:00:00')
      and txn_type = 'CREDIT'
),
enriched as (
    -- Ranking and gap features
    select
        id,
        account_id,
        amount,
        merchant,
        is_flagged,
        txn_type_prefix,
        resolved_merchant,
        row_num,
        txn_type_merchant_key,
        round(amount / sum(amount) over (partition by merchant), 4) as amount_share,
        row_number() over (partition by account_id order by row_num, id) as row_seq
    from base
),
summary as (
    -- Aggregate with conditional metrics
    select
        case when row_seq >= 7 then 'LARGE' else 'SMALL' end as row_seq_tier,
        merchant,
        count(*) filter (where merchant = '7-Eleven') as 7_eleven_rows,
        count_if(merchant = 'Shell') as shell_events,
        sort_array(collect_set(merchant)) as merchant_list
    from enriched
    group by case when row_seq >= 7 then 'LARGE' else 'SMALL' end, merchant
)
select
    row_seq_tier,
    merchant,
    shell_events,
    merchant_list
from summary;
