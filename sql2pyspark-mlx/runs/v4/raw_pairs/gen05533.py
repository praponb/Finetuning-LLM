from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("weight_kg").desc(), "id")
w_hist = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
    )
)

result = (
    flattened_attrs
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("weight_kg_sum_30m", F.sum("weight_kg").over(w_hist))
)
