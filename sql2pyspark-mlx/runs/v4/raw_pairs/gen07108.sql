with deduped as (select *, rank() over (partition by channel order by starts_on) as row_num from promotions qualify row_num = 1) select id, promo_code, ends_on, row_num from deduped;
