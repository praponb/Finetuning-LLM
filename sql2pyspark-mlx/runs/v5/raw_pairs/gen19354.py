from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("created_at", "id")
w_recent = Window.partitionBy("customer_id", "ship_city").orderBy(F.col("created_at").desc(), "id")
w4 = Window.partitionBy("ship_city").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.col("ship_to.city").alias("ship_city"),
        F.col("attributes")["device"].alias("device"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "customer_id",
        "created_at",
        "has_weight_kg",
        F.last("device", ignorenulls=True).over(w).alias("last_known_device"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.first("device", ignorenulls=True).over(w4).alias("first_known_device"),
    )
)
