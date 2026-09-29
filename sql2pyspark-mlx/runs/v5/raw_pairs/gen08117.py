from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date")
w2 = Window.partitionBy("city", "customer_id").orderBy(F.col("shipping_fee").desc_nulls_first())
w3 = Window.partitionBy("customer_id").orderBy(F.col("amount").desc_nulls_first())

deduped = (
    df_orders
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

kept_keys = (
    deduped
    .select("id")
    .intersect(
        df_orders
        .filter(F.col("discount") > 0.35)
        .select("id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.customer_id",
        "a.amount",
        "a.shipping_fee",
        "a.rn",
        F.col("p.city"),
        F.col("p.name"),
        F.col("p.signup_date"),
    )
)

with_windows = (
    with_dims
    .filter(F.year("signup_date") >= 2023)
    .select(
        "*",
        F.cume_dist().over(w2).alias("shipping_fee_cume_dist"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
    )
)

joined = (
    with_windows
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.segment"))
)

result = (
    joined
    .groupBy(
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
        "customer_id",
        F.when(F.col("amount_cume_dist") >= 0.71, "HIGH").otherwise("LOW").alias("amount_cume_dist_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.round(F.sum("amount_cume_dist") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("amount_cume_dist_per_tokyo"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.round(F.sum("amount_cume_dist") / F.nullif(F.count(F.when(F.col("segment") == "Consumer", 1)), F.lit(0)), 2).alias("amount_cume_dist_per_consumer"),
    )
    .filter(F.col("cnt") >= 1)
    .orderBy(F.col("week_start").desc())
)
