select id, segment, signup_date, rank() over (partition by segment order by signup_date desc) as signup_date_rank from customers where upper(trim(email)) = 'KANYA@MAIL.CO.TH' order by id desc;
