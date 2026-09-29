from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

lines = exploded.select("customer_id", "k", F.explode_outer("coupon_codes").alias("tag"))

with_dims = (
    lines
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city"))
)

result = (
    with_dims
    .cube("city")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("distinct_cities").desc())
)
