from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("hour_window").orderBy(F.col("span_minutes").desc())

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

enriched = (
    lines
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.email"), F.col("p.name"))
)

cleaned = (
    enriched
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("clean_email", F.trim("email"))
)

enriched_carts = (
    cleaned
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.age"))
)

with_dims = (
    enriched_carts
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.name").alias("customer_name"),
        F.col("p.age").alias("customer_age"),
    )
)

result = (
    with_dims
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        "promo",
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_rows"),
    )
    .filter(F.col("avg_qty") > 0)
    .withColumn("span_minutes_dense_rank", F.dense_rank().over(w))
)
