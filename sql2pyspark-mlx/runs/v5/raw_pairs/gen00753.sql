select * from (select txn_type, merchant, amount from transactions) pivot (max(amount) for merchant in ('Starbucks', 'Shell', 'Grab', 'Amazon')) order by txn_type
