from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category", "brand").orderBy(F.col("cost").desc_nulls_last(), "id")

cleaned = (
    df_products
    .filter(F.col("price") > 450)
    .select(
        "*",
        F.round((F.col("price") * 0.9), 2).alias("price_scaled"),
        F.when(F.col("category") == "Fashion", 1).otherwise(0).alias("is_fashion"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

deduped = (
    latest_match
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "price_scaled",
        F.when(F.col("product_name").rlike("^(Backpack XL|Coffee Maker)$"), "GROUP_A").otherwise("GROUP_B").alias("product_name_zone"),
    )
    .orderBy(F.col("stock_qty").asc_nulls_first(), "product_name_zone")
)
