from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("launch_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("launch_date").desc_nulls_last(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "product_id",
        "unit_price",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
        F.when(F.col("unit_price") >= 330, "High").when(F.col("unit_price") >= 80, "Medium").otherwise("Low").alias("unit_price_band"),
    )
)

with_dims = (
    filtered
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.launch_date"), F.col("d.product_name"), F.col("d.cost"))
)

ranked = (
    with_dims
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.launch_date").alias("product_launch_date"))
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .withColumn("year_start", F.date_trunc("year", "product_launch_date"))
    .groupingSets(
        [["product_id", "year_start"], ["product_id"], []],
        "product_id",
        "year_start",
    )
    .agg(
        F.percentile_approx("line_total", [0.25, 0.5, 0.75]).alias("line_total_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.grouping("product_id").alias("grouping_level"),
    )
    .orderBy(F.col("year_start").desc_nulls_last())
)
