select status
from accounts
where account_type in ('SAVINGS', 'LOAN', 'CHECKING')
except
select status
from shipments
where carrier = 'FedEx';
