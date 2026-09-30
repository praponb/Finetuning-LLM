from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "attr_key").orderBy("created_at", "id", "attr_key")
w2 = Window.partitionBy("customer_id").orderBy("created_at")
w3 = Window.partitionBy("attr_key").orderBy(F.col("dedup_rank").desc(), "id", "attr_key")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

deduped = (
    flattened
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest = (
    deduped
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

latest_carts = (
    latest
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "created_at", "attr_key", "v", "dedup_rank")
)

with_dims = (
    latest_carts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.id", "a.customer_id", "a.attr_key", F.col("d.city"))
)

result = (
    with_dims
    .groupBy("customer_id", "attr_key")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.count(F.when(F.col("attr_key") == "weight_kg", 1)).alias("weight_kg_rows"),
        F.count("*").alias("cnt"),
    )
    .orderBy("cnt")
)
