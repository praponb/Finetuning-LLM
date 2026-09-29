from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "key").orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

result = (
    lines
    .filter(F.col("created_at") < "2024-12-01")
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "key",
        F.first("key", ignorenulls=True).over(w).alias("first_known_key"),
        F.first("key", ignorenulls=True).over(w2).alias("first_known_key_2"),
        F.first("key", ignorenulls=True).over(w3).alias("first_known_key_3"),
    )
)
