from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("carrier") == "Kerry") & (F.col("delivered_date") <= "2023-06-01")) | (F.col("carrier") == "Flash"))
    .groupBy(
        F.when(F.col("cost") >= 79, "high").when(F.col("cost") >= 36, "mid").otherwise("low").alias("cost_bucket"),
        "status",
    )
    .agg(F.max("cost").alias("max_cost"))
)
