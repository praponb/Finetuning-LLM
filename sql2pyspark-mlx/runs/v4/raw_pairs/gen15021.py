from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id", "product_id").orderBy("sale_date_weekofyear", "id")
w2 = Window.orderBy("sale_date_weekofyear", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("sale_date_weekofyear", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_store_sales
    .filter(F.col("units") >= 37)
    .withColumn("sale_date_weekofyear", F.weekofyear("sale_date"))
)

enriched = (
    base
    .withColumn("next_units", F.lead("units", 1, 0).over(w_hist))
    .withColumn("last_known_units", F.last("units", ignorenulls=True).over(w2))
    .withColumn("first_known_revenue", F.first("revenue", ignorenulls=True).over(w3))
)

valid = (
    enriched
    .filter(F.col("sale_date") > F.lit("2023-03-07").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "sale_date_weekofyear",
        "first_known_revenue",
        F.round("last_known_units", 0).alias("last_known_units_rounded"),
    )
)

with_dims = (
    valid
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.product_name"))
)

result = (
    with_dims
    .groupBy(F.date_trunc("WEEK", "sale_date").alias("week_start"))
    .agg(
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("last_known_units_rounded", 0.9).alias("last_known_units_rounded_p90"),
    )
    .filter(F.col("last_known_units_rounded_p90") > 0)
    .orderBy("distinct_ids")
)
