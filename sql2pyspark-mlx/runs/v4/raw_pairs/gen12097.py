from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("product_id")
w6 = Window.partitionBy("store_id").orderBy(F.col("first_known_revenue").desc(), "id")
w7 = Window.partitionBy("product_id", "store_id").orderBy("sqft")
w8 = Window.partitionBy("product_id", "store_id").orderBy(F.col("event_seq").asc_nulls_last(), "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

enriched = (
    df_store_sales
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.sqft"), F.col("s.store_name"))
)

with_windows = (
    enriched
    .withColumn("first_known_revenue", F.first("revenue", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("revenue_avg_6_prior", F.avg("revenue").over(w3))
    .withColumn("revenue_avg_5_prior", F.avg("revenue").over(w4))
)

scored = (
    with_windows
    .select(
        "*",
        F.round((F.col("first_known_revenue") / F.sum("first_known_revenue").over(w5)), 4).alias("first_known_revenue_share"),
        F.row_number().over(w6).alias("row_seq"),
        F.cume_dist().over(w7).alias("sqft_cume_dist"),
    )
)

latest = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    latest
    .filter(F.col("units") > 16)
    .withColumn("store_name_normalized", F.upper(F.trim("store_name")))
)

joined = (
    cleaned
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.product_name"), F.col("d.launch_date"))
)

kept_keys = (
    joined
    .select("store_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("units").isNull())
        .select("store_id", "id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "sqft",
        "store_name",
        "first_known_revenue",
        F.col("avg_inventory_quantity"),
    )
    .orderBy(F.col("id").desc(), "sale_date")
)
