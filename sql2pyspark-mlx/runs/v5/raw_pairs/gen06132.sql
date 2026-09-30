select
    id,
    context,
    cast((status_code & 2) != 0 as int) as has_fault,
    get_json_object(payload_json, '$.user.tier') as user_tier,
    get_json_object(payload_json, '$.duration') as duration_text
from app_logs;
