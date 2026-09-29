from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

enriched = (
    flattened
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.age"), F.col("d.city"), F.col("d.email"))
)

cleaned = (
    enriched
    .filter(F.col("city").isNotNull() & (F.col("age") <= 46))
    .select("*", F.round((F.col("age") * 0.9), 2).alias("age_adjusted"))
)

kept_keys = (
    cleaned
    .select("customer_id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

matched_keys = (
    verified
    .select("id", "customer_id")
    .intersect(
        df_carts
        .filter(F.hour("created_at").between(10, 15))
        .select("id", "customer_id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("customer_id", "k")
    .pivot("city", ["Chiang Mai", "Singapore", "London"])
    .agg(F.sum("age").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "customer_id",
        "k",
        F.col("Chiang Mai_value_sum").alias("chiang_mai_value_sum"),
        F.col("Chiang Mai_n").alias("chiang_mai_n"),
        F.col("Singapore_value_sum").alias("singapore_value_sum"),
        F.col("Singapore_n").alias("singapore_n"),
        F.col("London_value_sum").alias("london_value_sum"),
        F.col("London_n").alias("london_n"),
    )
)
