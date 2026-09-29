from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("attributes").alias("key", "value"))

enriched = (
    lines
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.country"), F.col("d.email"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.name"), F.col("c.country").alias("customer_country"))
)

result = (
    with_dims
    .select("id", "customer_id", "key", "value", "country")
    .orderBy(F.col("customer_id").desc_nulls_first())
)
