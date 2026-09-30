from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at_month").desc_nulls_last())
w2 = Window.partitionBy("customer_id")

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

base = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-01 18:00:00").cast("timestamp")) & F.col("promo").isNotNull())
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "price",
        F.hour("created_at").alias("created_at_hour"),
        F.round((F.col("qty") * 0.85), 2).alias("qty_adjusted"),
    )
)

base_carts = (
    base
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "created_at",
        "price",
        "created_at_hour",
        "qty_adjusted",
        F.coalesce("price", F.lit(0)).alias("price_filled"),
    )
)

cleaned = (
    base_carts
    .filter(F.col("created_at") >= F.lit("2025-03-02").cast("date"))
    .select(
        "customer_id",
        "price",
        "qty_adjusted",
        F.month("created_at").alias("created_at_month"),
    )
)

scored = (
    cleaned
    .withColumn("created_at_month_dense_rank", F.dense_rank().over(w))
    .withColumn("created_at_month_rank", F.rank().over(w))
    .withColumn("customer_id_stddev_samp_qty_adjusted", F.stddev_samp("qty_adjusted").over(w2))
)

with_dims = (
    scored
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.country"), F.col("b.email"))
)

result = (
    with_dims
    .select(
        "customer_id",
        "price",
        "created_at_month",
        "created_at_month_rank",
        "country",
        "email",
    )
    .orderBy(F.col("country").desc())
)
