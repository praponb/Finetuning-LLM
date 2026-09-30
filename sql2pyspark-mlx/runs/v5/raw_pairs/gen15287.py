from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost").between(31, 63) & (F.col("status") == "RETURNED")) | (F.col("cost") >= 83))
    .groupBy(
        F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"),
        "order_id",
        "status",
    )
    .agg(F.max("cost").alias("max_cost"))
)
