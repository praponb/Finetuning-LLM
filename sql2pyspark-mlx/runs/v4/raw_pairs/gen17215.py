from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "key").orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key", "customer_id").orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("created_at", "id", "key")

exploded = (
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
    exploded
    .withColumn("last_known_key", F.last("key", ignorenulls=True).over(w))
    .withColumn("first_known_key", F.first("key", ignorenulls=True).over(w))
    .withColumn("last_known_key_2", F.last("key", ignorenulls=True).over(w2))
    .withColumn("first_known_key_2", F.first("key", ignorenulls=True).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)
