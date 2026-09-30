from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .groupBy("k")
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
    .orderBy("k")
)
