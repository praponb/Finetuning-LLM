from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("uk_count").desc())

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select("id", "customer_id", F.col("item.product_id"), F.col("item.price"))
)

valid = (
    lines
    .filter(F.col("customer_id").isin([8, 21, 30, 38]))
    .withColumn("product_id_filled", F.coalesce("product_id", F.lit(0)))
)

joined = (
    valid
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.city"), F.col("b.country"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.price",
        "src.product_id_filled",
        "src.country",
        F.col("p.signup_date"),
    )
)

result = (
    with_dims
    .groupBy(
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
        "customer_id",
    )
    .agg(
        F.count_if(F.col("country") == "UK").alias("uk_events"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
    .filter(F.col("uk_events") > 1)
    .withColumn("uk_count_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("uk_events").desc())
)
