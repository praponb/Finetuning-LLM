SELECT merchant, SUM(CASE WHEN merchant = 'Lazada' THEN amount ELSE 0 END) AS lazada_amount
FROM transactions
GROUP BY merchant
ORDER BY merchant;
