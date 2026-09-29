SELECT *, LAST_DAY(opened_date) opened_date_month_end FROM accounts WHERE account_type IN ('CHECKING', 'CREDIT') AND balance < 44000
