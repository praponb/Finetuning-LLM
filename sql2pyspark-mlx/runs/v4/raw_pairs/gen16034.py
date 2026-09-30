from pyspark.sql import functions as F

src = df_shipments.filter(F.col("order_id").isNotNull() | (F.col("status") == "LOST"))

status_summary = (
    src
    .groupBy("status", "carrier")
    .agg(
        F.max("cost").alias("max_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)

result = (
    status_summary
    .filter(F.col("cost_range") < 68)
    .orderBy("cost_range", F.col("max_cost").desc())
)
