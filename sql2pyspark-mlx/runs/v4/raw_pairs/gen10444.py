from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("category_length").desc())
w2 = Window.partitionBy("category").orderBy("category_length", "id")
w3 = Window.partitionBy("brand")
w4 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("brand").orderBy("cost", "id")
w6 = Window.partitionBy("supplier_id").orderBy("category_length_percent_rank", "id")
w_ordered = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("brand", "category").orderBy(F.col("category_length_percent_rank").asc_nulls_last())

base = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-10-14"))) & F.col("brand").isNotNull() & F.col("price").isNotNull())
    .select("*", F.length("category").alias("category_length"))
)

enriched = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("category_length_percent_rank"),
        F.lead("price", 2).over(w2).alias("next_price"),
    )
)

enriched_products = (
    enriched
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
        F.first("category_length", ignorenulls=True).over(w4).alias("first_known_category_length"),
        F.lag("price", 1, 0).over(w5).alias("prev_price"),
    )
)

with_windows = (
    enriched_products
    .withColumn("next_price_share", F.lead("price_share", 1).over(w6))
    .withColumn("cumulative_category_length_percent_rank", F.sum("category_length_percent_rank").over(w_ordered))
    .withColumn("category_length_percent_rank_cume_dist", F.cume_dist().over(w8))
)

kept = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") == 5), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.when(F.col("brand").rlike("^(Globex|Stark)$"), "GROUP_B").when(F.col("brand").rlike("^(Acme|Initech)$"), "EXTENDED").otherwise("PRIORITY").alias("brand_zone"),
    )
    .orderBy(F.col("brand").desc(), "category")
)
