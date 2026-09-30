from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "attributes",
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
    )
)
