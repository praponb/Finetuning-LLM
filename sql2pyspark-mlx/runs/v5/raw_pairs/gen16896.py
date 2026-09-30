from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("elem.product_id"),
        F.col("elem.price"),
    )
)

result = (
    lines
    .cube(
        "product_id",
        "customer_id",
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("product_id").desc())
)
