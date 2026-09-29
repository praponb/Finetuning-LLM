from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        F.col("item.product_id"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

valid = (
    exploded
    .filter(F.col("product_id") != 7)
    .select("*", F.ceil("product_id").alias("product_id_whole"))
)

joined = (
    valid
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.customer_id", "e.price", F.col("p.age"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.customer_id", "a.age", F.col("c.credit_limit"), F.col("c.segment"))
)

joined_carts = (
    with_dims
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select(
        "s.customer_id",
        "s.age",
        F.col("b.email"),
        F.col("b.city"),
        F.col("b.signup_date"),
    )
)

enriched = (
    joined_carts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("a.*", F.col("c.credit_limit"), F.col("c.name"))
)

joined_carts_2 = (
    enriched
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .filter(F.col("c.name").like("%Lee%"))
    .select(
        "src.customer_id",
        "src.city",
        "src.credit_limit",
        F.col("c.age").alias("customer_age"),
        F.col("c.city").alias("customer_city"),
    )
)

daily = (
    joined_carts_2
    .groupBy(
        F.when(F.col("customer_age") >= 57, "PREMIUM").otherwise("STANDARD").alias("customer_age_tier"),
    )
    .agg(
        F.percentile_approx("customer_age", 0.5).alias("customer_age_p50"),
        F.sort_array(F.collect_set("customer_city")).alias("customer_city_list"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sort_array(F.collect_set("city")).alias("city_list_2"),
        F.count("*").alias("event_count"),
    )
)

result = (
    daily
    .select("customer_age_tier", "city_list_2", "event_count")
    .orderBy(F.col("customer_age_tier").desc_nulls_first())
)
