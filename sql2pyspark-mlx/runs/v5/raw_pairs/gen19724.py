from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element", "customer_id")

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "rec"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("rec.qty"),
        F.col("rec.price"),
    )
)

flattened = (
    lines
    .select(
        "id",
        "customer_id",
        "pos",
        "qty",
        F.explode("coupon_codes").alias("element"),
    )
)

enriched = (
    flattened
    .filter(F.col("pos").isNotNull())
    .select("id", "customer_id", F.count("pos").over(w).alias("element_count_pos"))
)

enriched_carts = (
    enriched
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.credit_limit"), F.col("p.country"))
)

base = (
    enriched_carts
    .filter(F.col("country") == "JP")
    .withColumn("credit_limit_whole", F.ceil("credit_limit"))
)

result = (
    base
    .select("id", "customer_id", "credit_limit", "credit_limit_whole")
    .orderBy(F.col("credit_limit").desc_nulls_last())
)
