from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("stock_qty").desc_nulls_first())
w2 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc())
w3 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc_nulls_last())

cleaned = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-06-01").cast("date")) & F.col("brand").isNotNull() & (F.col("cost") >= 720))
    .withColumn("launch_date_plus_41d", F.date_add("launch_date", 41))
    .withColumn("cost_int", F.col("cost").cast("int"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("DIRECT")))
)

matched_keys = (
    cleaned
    .select("category")
    .intersect(
        df_products
        .filter(F.col("cost") <= 330)
        .select("category")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("stock_qty_cume_dist", F.cume_dist().over(w))
    .withColumn("stock_qty_rank", F.rank().over(w2))
    .withColumn("stock_qty_rank_2", F.rank().over(w3))
)

valid = (
    scored
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-03-22"))) & F.col("brand").isNotNull())
    .withColumn("brand_product_name_key", F.concat_ws(",", "brand", "product_name"))
)

result = (
    valid
    .select(
        "id",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "launch_date_plus_41d",
        "stock_qty_cume_dist",
        "stock_qty_rank_2",
        "brand_product_name_key",
    )
    .orderBy(F.col("stock_qty").desc(), F.col("stock_qty_rank_2").desc())
)
