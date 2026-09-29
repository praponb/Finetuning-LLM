from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "customer_id").orderBy(F.col("signup_date").desc(), "id", "k")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "v"),
    )
)

enriched = (
    exploded
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(~F.col("p.is_active"))
    .select("a.*", F.col("p.signup_date"), F.col("p.email"), F.col("p.city"))
)

metrics = enriched.select("*", F.row_number().over(w).alias("event_seq"))

base = (
    metrics
    .filter(F.col("signup_date") <= "2025-01-01")
    .withColumn("k_lower", F.lower("k"))
)

with_dims = (
    base
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.name"),
        F.col("d.signup_date").alias("customer_signup_date"),
    )
)

result = (
    with_dims
    .groupBy(
        F.date_trunc("QUARTER", "customer_signup_date").alias("quarter_start"),
        "customer_id",
    )
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("k").alias("distinct_ks"),
        F.count(F.when(F.col("k") == "device", 1)).alias("device_count"),
        F.count(F.when(F.col("k") == "weight_kg", 1)).alias("weight_kg_rows"),
    )
    .orderBy(F.col("weight_kg_rows").desc_nulls_last())
)
