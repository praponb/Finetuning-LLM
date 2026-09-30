from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~(F.col("order_id").isin([8, 26]) & ((F.col("shipped_date") < "2024-03-01") | (F.col("shipped_date") <= "2024-02-14"))))
    .orderBy("delivered_date", F.col("status").desc())
)
