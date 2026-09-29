select id, merchant, amount, round(amount / sum(amount) over (partition by merchant), 3) amount_share, row_number() over (partition by merchant order by amount desc, id) amount_row from transactions;
