from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").asc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("latest_rank").desc())

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", F.col("quantity").cast("string").alias("quantity_num"))
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest_order_items = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

latest_order_items_2 = (
    latest_order_items
    .withColumn("latest_rank_2", F.rank().over(w3))
    .filter(F.col("latest_rank_2") == 1)
    .drop("latest_rank_2")
)

joined = (
    latest_order_items_2
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.cost"), F.col("d.brand"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.category") == "Grocery")
    .select("s.*", F.col("d.launch_date"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("cost") >= 770, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.avg(F.when(F.col("brand") == "Stark", F.col("unit_price"))).alias("stark_avg_unit_price"),
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("stark_avg_unit_price") > 0)
)
