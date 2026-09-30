from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("attr_key", "customer_id")
w3 = Window.partitionBy("attr_key", "customer_id").orderBy(F.col("customer_id_stddev_samp_attr_key_length").desc_nulls_last())
w4 = Window.partitionBy("customer_id").orderBy(F.col("customer_id_stddev_samp_attr_key_length").desc_nulls_first())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

cleaned = (
    lines
    .filter(F.col("customer_id").isin([9, 17, 31]))
    .withColumn("attr_key_length", F.length("attr_key"))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("DIRECT")))
)

kept_keys = (
    cleaned
    .select("customer_id")
    .subtract(
        df_carts
        .filter(F.to_date("created_at") >= "2024-07-02")
        .select("customer_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.sum("attr_key_length").over(w).alias("customer_id_sum_attr_key_length"),
        F.stddev_samp("attr_key_length").over(w).alias("customer_id_stddev_samp_attr_key_length"),
    )
)

metrics_carts = (
    metrics
    .withColumn("attr_key_max_attr_key_length", F.max("attr_key_length").over(w2))
    .withColumn("customer_id_stddev_samp_attr_key_length_cume_dist", F.cume_dist().over(w3))
    .withColumn("customer_id_stddev_samp_attr_key_length_dense_rank", F.dense_rank().over(w4))
)

result = (
    metrics_carts
    .select(
        "id",
        "customer_id",
        "value",
        "attr_key_length",
        "attr_key_clean",
        "customer_id_sum_attr_key_length",
        "customer_id_stddev_samp_attr_key_length_cume_dist",
    )
)
