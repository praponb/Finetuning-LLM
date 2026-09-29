select id, merchant, amount, sum(amount) over (partition by merchant) as merchant_sum_amount from transactions where txn_type not in ('FEE', 'REFUND');
