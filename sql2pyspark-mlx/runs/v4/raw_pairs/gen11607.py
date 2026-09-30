from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").asc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())
w3 = Window.partitionBy("product_id")
w4 = Window.orderBy("units", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("product_id", "store_id")
w_ordered = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w7 = Window.partitionBy("store_id")

ranked = (
    df_store_sales
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest = (
    ranked
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w3)), 4))
    .withColumn("units_avg_3", F.avg("units").over(w4))
    .withColumn("corr_revenue_units", F.corr("revenue", "units").over(w5))
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w7)), 4))
)

enriched_store_sales = (
    enriched
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.city"), F.col("b.opened_date"), F.col("b.sqft"))
)

cleaned = (
    enriched_store_sales
    .filter(F.col("store_id").isin([1, 4, 5]))
    .withColumn("days_opened_date_to_sale_date", F.datediff("sale_date", "opened_date"))
    .withColumn("revenue_rounded", F.round("revenue", 0))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
)

enriched_store_sales_2 = (
    cleaned
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city").alias("store_city"))
)

with_dims = (
    enriched_store_sales_2
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.price"), F.col("p.product_name"), F.col("p.launch_date"))
)

grouped = (
    with_dims
    .groupBy(
        F.when(F.col("price") >= 1270, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
        "city",
    )
    .agg(
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.sort_array(F.collect_set("store_city")).alias("store_city_list"),
    )
)

result = (
    grouped
    .select(
        "price_tier",
        "city",
        "price_quartiles",
        "new_york_events",
        "store_city_list",
        F.when(F.col("city").rlike("^(London|Berlin)$"), "PRIORITY").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("city").asc_nulls_last(), "city_zone")
)
