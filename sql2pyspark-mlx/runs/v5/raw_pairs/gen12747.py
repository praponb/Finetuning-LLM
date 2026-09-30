from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
    .select("id", "customer_id", "pos", F.col("line.price"), F.col("line.promo"))
)

enriched = (
    flattened
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select(
        "e.customer_id",
        "e.pos",
        "e.promo",
        F.col("p.signup_date"),
        F.col("p.name"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.age"))
)

kept_keys = (
    with_dims
    .select("customer_id")
    .exceptAll(
        df_carts
        .filter(F.to_date("created_at") >= "2023-05-21")
        .select("customer_id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

matched_keys = (
    kept_rows
    .select("customer_id")
    .exceptAll(
        df_carts
        .filter(F.col("customer_id") == 25)
        .select("customer_id")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("customer_id", "promo")
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.avg("pos").alias("avg_pos"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("avg_pos") >= 0)
)
