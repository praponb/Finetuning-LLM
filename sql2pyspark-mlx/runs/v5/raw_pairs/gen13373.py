from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w3 = Window.orderBy("sqft", "id")
w4 = Window.orderBy(F.col("sqft").desc(), "id")
w5 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.product_id") == 9), "left_semi")
)

deduped = (
    not_flagged
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_name", "city", "region", "opened_date", "sqft")
)

with_latest = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.units").alias("store_sale_units"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

enriched = (
    with_latest
    .filter(F.month("store_sale_sale_date").isin([3, 8, 12]))
    .select(
        "*",
        F.lag("store_sale_units", 1).over(w3).alias("prev_store_sale_units"),
        F.row_number().over(w4).alias("row_seq"),
        F.last("region", ignorenulls=True).over(w5).alias("last_known_region"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "store_sale_sale_date",
        "last_known_region",
    )
)

result = (
    latest
    .rollup(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.when(F.col("sqft") >= 14576, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft_2"),
        F.sum("sqft").alias("total_sqft"),
        F.count_if(F.col("city") == "London").alias("london_events"),
    )
    .orderBy(F.col("avg_sqft").desc())
)
