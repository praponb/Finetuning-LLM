from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "category").orderBy(F.col("greatest_units_revenue").desc(), "id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id", "store_id")
w4 = Window.partitionBy("product_id").orderBy("greatest_units_revenue", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_store_sales
    .filter(F.col("revenue").between(1600, 3200))
    .select("*", F.greatest("units", "revenue").alias("greatest_units_revenue"))
)

enriched = (
    base
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.launch_date"), F.col("p.price"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.category"))
)

enriched_store_sales = (
    with_dims
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w2)), 4))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w3)), 4))
    .withColumn("revenue_avg_2", F.avg("revenue").over(w4))
)

enriched_store_sales_2 = (
    enriched_store_sales
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select(
        "a.*",
        F.col("p.launch_date").alias("product_launch_date"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
    )
)

enriched_store_sales_3 = (
    enriched_store_sales_2
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.opened_date"), F.col("d.sqft"), F.col("d.region"))
)

result = (
    enriched_store_sales_3
    .withColumn("greatest_units_revenue_tier", F.when(F.col("greatest_units_revenue") >= 68, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["product_id", "greatest_units_revenue_tier"], ["product_id"], []],
        "product_id",
        "greatest_units_revenue_tier",
    )
    .agg(
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.sum(F.when(F.col("brand") == "Stark", F.col("price_share")).otherwise(0)).alias("stark_price_share"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("product_id").desc())
)
