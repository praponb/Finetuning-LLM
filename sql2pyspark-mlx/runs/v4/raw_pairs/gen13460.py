from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"),
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
    )
    .agg(
        F.countDistinct("cost").alias("unique_orders"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_flash"),
    )
    .orderBy(F.col("unique_orders").desc(), F.col("delivered_month").desc())
)
