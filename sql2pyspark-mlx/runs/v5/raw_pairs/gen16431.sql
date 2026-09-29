select merchant, lower(merchant) as merchant_lower from transactions where txn_type <> 'CREDIT' order by merchant_lower asc;
