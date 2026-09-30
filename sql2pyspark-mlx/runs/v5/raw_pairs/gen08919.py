from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("max_item_price", "id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("none")).alias("device_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "customer_id",
        "created_at",
        "max_item_price",
        F.max("max_item_price").over(w_grp).alias("running_max_max_item_price"),
        F.min("max_item_price").over(w_grp).alias("running_min_max_item_price"),
        F.count("max_item_price").over(w_grp).alias("running_count_max_item_price"),
        F.avg("max_item_price").over(w_grp).alias("running_avg_max_item_price"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
