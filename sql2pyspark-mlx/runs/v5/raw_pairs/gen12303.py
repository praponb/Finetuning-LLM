from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("attributes").alias("attr_key", "v"))

filtered = (
    exploded
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("device"))))
    .withColumn("attr_key_length", F.length("attr_key"))
    .withColumn("attr_key_lower", F.lower("attr_key"))
)

joined = (
    filtered
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("src.*", F.col("b.age"), F.col("b.email"), F.col("b.credit_limit"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.segment") != "Corporate")
    .select("e.*", F.col("b.country"))
)

with_dims_carts = (
    with_dims
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        "a.*",
        F.col("c.segment"),
        F.col("c.credit_limit").alias("customer_credit_limit"),
    )
)

result = (
    with_dims_carts
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["city", "attr_key"], ["city"], []],
        F.col("d.city").alias("city"),
        F.col("src.attr_key").alias("attr_key"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("src.attr_key_length")).alias("total_attr_key_length"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("attr_key").asc_nulls_last())
)
