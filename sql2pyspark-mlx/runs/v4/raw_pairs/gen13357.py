from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode("attributes").alias("k", "v"))

result = (
    flattened
    .select(
        "id",
        "k",
        F.when(F.col("k").rlike("^(device|weight_kg)$"), "GROUP_B").when(F.col("k").rlike("^(utm_source|gift)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("k_zone"),
    )
)
