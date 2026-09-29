from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

result = (
    exploded
    .select("id", "k")
    .orderBy(F.col("k").asc_nulls_first(), "id")
)
