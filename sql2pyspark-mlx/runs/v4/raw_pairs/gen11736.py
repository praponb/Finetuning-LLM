from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-04-21").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("cost") >= 97, "High").when(F.col("cost") >= 14, "Medium").otherwise("Low").alias("cost_level"),
        F.add_months("shipped_date", 6).alias("shipped_date_plus_months"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("YEAR", "shipped_date_plus_months").alias("year_start"))
    .agg(
        F.stddev_samp("cost").alias("cost_stddev"),
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.stddev_samp("cost").alias("cost_stddev_2"),
        F.stddev_samp("cost").alias("cost_stddev_3"),
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_rows"),
    )
    .orderBy("cost_stddev_3")
)
