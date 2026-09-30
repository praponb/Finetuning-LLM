select txn_type, merchant
from transactions
where merchant in ('Amazon', 'Starbucks', 'Lazada')
except
select txn_type, merchant
from transactions
where amount >= 3700
order by txn_type
