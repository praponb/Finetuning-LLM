from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("weight_kg", "id")
w2 = Window.partitionBy("customer_id").orderBy("weight_kg", "id")
w3 = Window.orderBy(F.col("weight_kg").desc(), "id")

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id").isin([15, 34, 39]))
    .select(
        "id",
        "customer_id",
        "ship_to",
        "attributes",
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.forall("items", lambda x: x["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(4).over(w3).alias("weight_kg_bucket"),
    )
)
