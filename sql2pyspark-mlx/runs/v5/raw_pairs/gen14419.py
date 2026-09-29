from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.size("coupon_codes") > 1)
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("none")).alias("utm_source_or_default"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
    )
)

result = (
    parsed
    .groupBy("customer_id")
    .agg(
        F.percentile_approx("bulk_lines", 0.95).alias("bulk_lines_p95"),
        F.percentile_approx("bulk_lines", [0.25, 0.5, 0.75]).alias("bulk_lines_quartiles"),
        F.round(F.avg("bulk_lines"), 2).alias("avg_bulk_lines"),
        F.percentile_approx("bulk_lines", [0.25, 0.5, 0.75]).alias("bulk_lines_quartiles_2"),
        F.sum("bulk_lines").alias("total_bulk_lines"),
        F.sum("bulk_lines").alias("total_bulk_lines_2"),
    )
    .orderBy(F.col("total_bulk_lines").asc_nulls_last())
)
