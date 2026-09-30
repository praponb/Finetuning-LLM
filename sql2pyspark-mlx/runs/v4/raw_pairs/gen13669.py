from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("attr_key", "attr_value"))
)

result = (
    flattened
    .select(
        "id",
        "created_at",
        "attr_key",
        "attr_value",
        F.when(F.col("attr_key").rlike("^(utm_source|weight_kg)$"), "GROUP_B").when(F.col("attr_key").rlike("^(device|gift)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("attr_key_zone"),
    )
    .orderBy("created_at")
)
