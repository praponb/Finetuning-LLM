from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("year_start").orderBy(F.col("distinct_ids").desc())

flattened = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

with_dims = (
    flattened
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.id", "a.customer_id", "a.tag", F.col("c.signup_date"), F.col("c.name"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.age") > 47)
    .select("a.*", F.col("b.city"))
)

with_dims_carts = (
    enriched
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.signup_date").alias("customer_signup_date"))
)

result = (
    with_dims_carts
    .groupBy(
        F.date_trunc("YEAR", "signup_date").alias("year_start"),
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("distinct_ids_dense_rank", F.dense_rank().over(w))
    .orderBy("activity_month")
)
