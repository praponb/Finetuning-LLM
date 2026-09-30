from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_first(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([1, 4, 20]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "product_id")
)

joined = (
    deduped
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.brand") == "Initech")
    .select("e.id", "e.product_id", F.col("p.category"))
)

valid = (
    joined
    .filter(F.col("category").isNotNull())
    .withColumn("category_filled", F.coalesce("category", F.lit("N/A")))
    .withColumn("category_clean", F.coalesce("category", F.lit("UNKNOWN")))
)

valid_order_items = (
    valid
    .filter(F.col("category").isin(["Fashion", "Books"]))
    .select(
        "id",
        "product_id",
        "category",
        "category_filled",
        "category_clean",
        F.substring("category", 1, 4).alias("category_prefix"),
        F.coalesce("category", F.lit("NONE")).alias("resolved_category"),
    )
)

result = (
    valid_order_items
    .groupBy("product_id")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") > 1)
)
