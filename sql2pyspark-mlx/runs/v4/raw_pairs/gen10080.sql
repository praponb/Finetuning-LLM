SELECT DISTINCT merchant
FROM transactions
WHERE is_flagged = TRUE
ORDER BY merchant;
