from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", F.explode_outer("attributes").alias("k", "v"))

result = (
    flattened
    .groupBy("k")
    .agg(
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
)
