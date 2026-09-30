from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "brand").orderBy(F.col("quantity").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isin([10, 21]))
    .select("*", (F.col("unit_price") + F.col("quantity")).alias("line_total"))
)

with_dims = (
    filtered
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.cost"), F.col("p.brand"))
)

latest = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

joined = (
    latest
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.amount"), F.col("o.discount"))
)

pivoted = (
    joined
    .groupBy("product_id")
    .pivot("brand", ["Initech", "Umbrella"])
    .agg(F.avg("amount").alias("amount"))
)

result = (
    pivoted
    .select("product_id", "Initech", "Umbrella")
    .orderBy("product_id")
)
