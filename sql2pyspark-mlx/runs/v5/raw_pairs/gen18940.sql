select 'store_sale' as record_type, sale_date from store_sales union all select 'account' as record_type, opened_date from accounts where account_type in ('CHECKING', 'LOAN', 'CREDIT');
