from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value").orderBy(F.col("created_at_quarter").asc_nulls_last())
w2 = Window.partitionBy("value").orderBy(F.col("created_at_quarter").desc_nulls_first())

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

flattened_carts = (
    flattened
    .select("id", "created_at", "v", F.explode_outer("coupon_codes").alias("value"))
)

base = (
    flattened_carts
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.col("value").isNotNull() & F.hour("created_at").between(9, 14))
    .withColumn("created_at_quarter", F.quarter("created_at"))
    .withColumn("resolved_value", F.coalesce("value", F.lit("DIRECT")))
)

scored = (
    base
    .select(
        "*",
        F.cume_dist().over(w).alias("created_at_quarter_cume_dist"),
        F.cume_dist().over(w2).alias("created_at_quarter_cume_dist_2"),
    )
)

filtered = (
    scored
    .filter(F.col("value").isNotNull())
    .select(
        "created_at",
        "resolved_value",
        "created_at_quarter_cume_dist_2",
        F.length("value").alias("value_length"),
        (F.col("created_at_quarter_cume_dist_2") + F.col("created_at_quarter")).alias("created_at_quarter_cume_dist_2_plus_created_at_quarter"),
    )
)

result = (
    filtered
    .select("value_length", "created_at_quarter_cume_dist_2_plus_created_at_quarter")
    .orderBy(
        F.col("created_at_quarter_cume_dist_2_plus_created_at_quarter").asc_nulls_first(),
    )
)
