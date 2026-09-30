from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") <= 59) & F.col("status").isin(["IN_TRANSIT", "LOST", "DELIVERED"]))
    .groupBy(
        F.when(F.col("cost") >= 86, "high").when(F.col("cost") >= 19, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.max("delivered_date").alias("last_delivered_date"),
        F.min("shipped_date").alias("first_shipped_date"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("max_cost") >= 80)
    .orderBy("first_shipped_date", "last_delivered_date")
)
