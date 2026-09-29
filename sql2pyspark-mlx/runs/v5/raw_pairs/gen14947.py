from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id").orderBy("price", "id")
w_ordered_2 = Window.partitionBy("product_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("price").desc_nulls_first())
w4 = Window.partitionBy("product_id")
w5 = Window.orderBy(F.col("product_id_sum_price").desc())
w6 = Window.orderBy(F.col("price_rank").desc(), "id")

joined = (
    df_inventory
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.quantity",
        F.col("p.price"),
        F.col("p.cost"),
        F.col("p.product_name"),
    )
)

scored = (
    joined
    .withColumn("prev_cost", F.lag("cost").over(w_ordered))
    .withColumn("next_cost", F.lead("cost").over(w_ordered))
    .withColumn("next_price", F.lead("price").over(w_ordered))
    .withColumn("prev_price", F.lag("price").over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w_ordered_2))
)

with_windows = (
    scored
    .select(
        "id",
        "price",
        "cost",
        "next_price",
        "event_seq",
        "last_known_quantity",
        F.rank().over(w_part).alias("price_rank"),
        F.sum("price").over(w4).alias("product_id_sum_price"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "price",
        "next_price",
        "event_seq",
        "product_id_sum_price",
        F.cume_dist().over(w5).alias("product_id_sum_price_cume_dist"),
        F.ntile(4).over(w6).alias("price_rank_bucket"),
    )
)

filtered = (
    enriched
    .filter(F.col("next_price").isNotNull())
    .select(
        "*",
        F.greatest("price_rank_bucket", "product_id_sum_price_cume_dist").alias("greatest_price_rank_bucket_product_id_sum_price_cume_dist"),
    )
)

result = (
    filtered
    .select(
        "id",
        "price",
        "next_price",
        "event_seq",
        "product_id_sum_price",
        "product_id_sum_price_cume_dist",
        "price_rank_bucket",
    )
    .orderBy(F.col("product_id_sum_price").desc(), "id")
    .limit(5)
)
