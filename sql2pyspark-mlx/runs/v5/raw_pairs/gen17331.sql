select round(stddev(balance), 2) as stddev_balance from accounts where account_type not in ('CREDIT', 'CHECKING') and account_type not in ('LOAN', 'CREDIT')
