from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-03-01", "2025-10-25") & (F.col("delivered_date") <= "2023-11-01"))
    .groupBy(F.month("shipped_date").alias("shipped_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0))) / F.count("*")), 1).alias("pct_kerry"),
        F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0)).alias("dhl_count"),
    )
    .orderBy(F.col("shipped_month").desc(), F.col("pct_kerry").desc())
    .limit(5)
)
