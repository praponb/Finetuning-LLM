from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

with_dims = (
    exploded
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select("s.customer_id", "s.created_at", F.col("b.name"))
)

filtered = (
    with_dims
    .filter(F.col("customer_id").isNotNull() & (F.length("name") < 13))
    .withColumn("name_title", F.initcap(F.trim("name")))
)

enriched = (
    filtered
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.customer_id", "e.name_title", F.col("d.email"), F.col("d.city"))
)

valid = (
    enriched
    .filter(F.col("city").isin(["Chiang Mai", "New York"]) & F.col("city").isNotNull())
    .withColumn("email_user", F.split(F.lower("email"), "@").getItem(0))
)

result = (
    valid
    .cube("customer_id", "city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.grouping("customer_id").alias("grouping_id"),
    )
    .filter(F.col("num_cities") > 1)
    .orderBy(F.col("customer_id").asc_nulls_first())
)
