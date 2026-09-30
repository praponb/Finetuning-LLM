from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.month("shipped_date").isin([4, 5, 9]) & F.col("order_id").isin([7, 13, 16, 22]))
)

summary = (
    src
    .groupBy("status")
    .agg(F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"))
)

result = (
    summary
    .filter(F.col("kerry_count") > 7)
    .select("status", "kerry_count")
)
