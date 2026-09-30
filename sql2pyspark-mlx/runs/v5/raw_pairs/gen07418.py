from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter(F.col("order_id").isin([10, 22, 28]) & F.col("carrier").isin(["Kerry", "UPS"]))
)

shipments_stats = (
    filtered_shipments
    .groupBy(
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.count("*").alias("num_shipments"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)

result = (
    shipments_stats
    .select("shipped_cohort", "num_shipments", "cost_range")
    .orderBy(F.col("shipped_cohort").desc())
)
