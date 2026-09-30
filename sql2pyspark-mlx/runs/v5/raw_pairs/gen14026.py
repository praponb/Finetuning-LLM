from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
        F.col("attributes").getItem("device").alias("device"),
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.size("items").alias("num_items"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.avg("bulk_lines").over(w).alias("running_avg_bulk_lines"),
        F.sum("bulk_lines").over(w).alias("running_sum_bulk_lines"),
        F.min("bulk_lines").over(w).alias("running_min_bulk_lines"),
    )
)
