from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .select("id", "customer_id", "created_at", "attr_key")
    .orderBy(F.col("created_at").desc(), F.col("id").desc())
)
