select
    id,
    merchant,
    is_flagged,
    account_id
from transactions
where txn_type = 'DEBIT'
order by account_id, merchant;
