from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc_nulls_last(), "id")
w_grp = Window.partitionBy("category").orderBy("price", "id")
w3 = Window.partitionBy("supplier_id", "category").orderBy(F.col("price").desc())

valid = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-08-25").cast("date"))
    .select(
        "*",
        F.initcap(F.trim("product_name")).alias("product_name_title"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "category",
        F.lead("stock_qty", 1).over(w_grp).alias("next_stock_qty"),
        F.cume_dist().over(w3).alias("price_cume_dist"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id") == 17), "left_semi")
)

result = (
    kept
    .withColumn("next_stock_qty_tier", F.when(F.col("next_stock_qty") >= 331, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["category", "next_stock_qty_tier"], ["category"]],
        "category",
        "next_stock_qty_tier",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_rows"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("next_stock_qty"))).alias("electronics_total_next_stock_qty"),
    )
)
