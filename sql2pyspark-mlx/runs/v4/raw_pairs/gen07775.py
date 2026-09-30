from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2024-02-22")
    .groupBy(
        F.when(F.col("cost") >= 82, "high").when(F.col("cost") >= 32, "mid").otherwise("low").alias("cost_bucket"),
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
    )
    .filter(F.col("lost_cost") >= 45)
    .orderBy("shipped_cohort")
)
