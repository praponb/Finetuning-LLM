from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["Singapore", "Bangkok"])
    .agg(F.sum("age"))
    .select(
        "segment",
        F.col("Singapore").alias("singapore"),
        F.col("Bangkok").alias("bangkok"),
    )
)
