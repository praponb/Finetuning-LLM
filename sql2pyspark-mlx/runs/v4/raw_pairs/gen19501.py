from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("price").asc_nulls_last(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock_weekofyear").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last())
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("product_id")
w6 = Window.partitionBy("store_id", "product_id").orderBy("event_seq", "id")

cleaned = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-08-02").cast("date")) & (F.col("quantity") != 108))
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM"))
    .withColumn("last_restock_weekofyear", F.weekofyear("last_restock"))
)

joined = (
    cleaned
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.stock_qty"), F.col("d.price"))
)

ranked = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .select(
        "*",
        F.ntile(3).over(w2).alias("last_restock_weekofyear_bucket"),
        F.cume_dist().over(w3).alias("quantity_cume_dist"),
        F.row_number().over(w4).alias("event_seq"),
        F.avg("stock_qty").over(w5).alias("product_id_avg_stock_qty"),
    )
)

ranked_inventory = (
    enriched
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

base = (
    ranked_inventory
    .filter(F.col("last_restock") > F.lit("2024-03-21").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "last_restock_label",
        "last_restock_weekofyear",
        "price",
        "dedup_rank",
        "quantity_cume_dist",
        "event_seq",
        "row_num",
        F.weekofyear("last_restock").alias("last_restock_weekofyear_2"),
        F.when(F.col("quantity") >= 232, "High").when(F.col("quantity") >= 91, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

joined_inventory = (
    base
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.opened_date"), F.col("b.store_name"), F.col("b.city"))
)

result = (
    joined_inventory
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
    )
)
