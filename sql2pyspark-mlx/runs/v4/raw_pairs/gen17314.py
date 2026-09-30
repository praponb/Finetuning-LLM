from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.size("items").alias("num_items"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("none")).alias("utm_source_or_default"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.sum("num_items").over(w).alias("cumulative_num_items"),
        F.sum("num_items").over(w).alias("running_num_items"),
        F.count("num_items").over(w2).alias("num_items_count_2h"),
    )
)
