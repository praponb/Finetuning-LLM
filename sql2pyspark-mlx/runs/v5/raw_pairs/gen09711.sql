SELECT merchant, SORT_ARRAY(COLLECT_SET(merchant)) merchant_list
FROM transactions
WHERE amount <= 3500 AND is_flagged = FALSE
GROUP BY merchant
HAVING COUNT(*) >= 5
ORDER BY merchant;
