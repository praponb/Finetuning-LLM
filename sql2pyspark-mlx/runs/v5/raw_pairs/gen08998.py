from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").asc_nulls_last(), "id")

latest = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "store_id", "product_id", "sale_date", "units")
)

base = (
    latest
    .filter(F.year("sale_date") >= 2025)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.col("units").cast("string").alias("units_str"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.store_name"))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.sqft"), F.col("p.opened_date"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("sqft") >= 14265, "LARGE").otherwise("SMALL").alias("sqft_tier"),
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
    )
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.bit_or("units").alias("units_bit_or_2"),
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
    )
)
