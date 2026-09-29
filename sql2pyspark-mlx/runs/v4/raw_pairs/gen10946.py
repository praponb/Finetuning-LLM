from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price").asc_nulls_last(), "id")
w_ordered = Window.partitionBy("order_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")

joined = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.order_date"), F.col("d.discount"))
)

latest = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .withColumn("order_id_avg_unit_price", F.avg("unit_price").over(w_ordered))
    .withColumn("unit_price_bucket", F.ntile(3).over(w3))
    .withColumn("order_id_max_discount", F.max("discount").over(w_ordered))
)

result = (
    with_windows
    .select("id", "order_id", "unit_price", "discount")
    .unpivot(["id", "order_id"], ["unit_price", "discount"], "metric_name", "value")
)
