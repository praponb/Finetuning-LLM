select
    a.status,
    t.txn_type,
    t.amount,
    if(t.is_flagged, 'Yes', 'No') as is_flagged_label
from accounts a
join transactions t on a.id = t.account_id
