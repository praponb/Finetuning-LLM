from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
        "carrier",
    )
    .agg(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"))
    .filter(F.col("in_transit_count") >= 3)
)
