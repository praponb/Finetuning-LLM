with unpacked as (
    -- Higher-order array functions
    select
        id,
        log_ts,
        status_code,
        payload_json,
        context,
        size(context) as num_context,
        coalesce(context['version'], 'unknown') as version_or_default
    from app_logs
)
-- Ranking and gap features
select
    id,
    log_ts,
    num_context,
    version_or_default,
    avg(status_code) over (order by status_code, id rows between 5 preceding and 1 preceding) as status_code_avg_5_prior,
    rank() over (order by num_context desc) as num_context_rank
from unpacked;
