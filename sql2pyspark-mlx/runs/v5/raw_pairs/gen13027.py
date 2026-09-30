from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["UPS", "Kerry"]) & (F.col("shipped_date") <= "2024-04-01") & F.month("shipped_date").isin([5, 8, 12]))
    .select("id", "shipped_date", "order_id", "carrier")
)
