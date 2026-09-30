from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("cost_tier", F.when(F.col("cost") >= 101, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "shipped_date"))
    .groupingSets(
        [["cost_tier", "activity_month"], ["cost_tier"]],
        "cost_tier",
        "activity_month",
    )
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("carrier") == "Kerry", 1)), F.lit(0)), 2).alias("cost_per_kerry"),
        F.avg(F.when(F.col("status") == "LOST", F.col("cost"))).alias("lost_avg_cost"),
        F.avg(F.when(F.col("status") == "DELIVERED", F.col("cost"))).alias("delivered_avg_cost"),
        F.max(F.when(F.col("status") == "RETURNED", F.col("cost"))).alias("returned_max_cost"),
    )
    .orderBy(F.col("returned_max_cost").desc_nulls_last())
)
