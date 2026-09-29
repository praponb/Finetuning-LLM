from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
    )
    .filter(F.col("delivered_count") < 4)
    .drop("delivered_count")
)
