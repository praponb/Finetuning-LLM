select
    accounts.id,
    accounts.account_type,
    accounts.opened_date,
    accounts.balance
from accounts
where accounts.id in (select account_id from transactions where merchant in ('Grab', '7-Eleven'));
