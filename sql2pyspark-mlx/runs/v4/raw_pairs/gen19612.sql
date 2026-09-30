SELECT merchant, TRIM(merchant) AS clean_merchant FROM transactions ORDER BY clean_merchant, merchant DESC;
