from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-10-07"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.trim("city").alias("clean_city"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id") == 8), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

result = (
    with_latest
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
        F.when(F.col("sqft") >= 13105, "HIGH").otherwise("LOW").alias("sqft_tier"),
    )
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") > 0)
    .orderBy(F.col("quarter_start").asc_nulls_last())
)
