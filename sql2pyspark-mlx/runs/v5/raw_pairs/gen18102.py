from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("attr_key")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

exploded = (
    flattened
    .select(
        "id",
        "created_at",
        "attr_key",
        "attr_value",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

cleaned = (
    exploded
    .filter(F.col("tag").isNotNull() & (F.col("created_at") > F.to_date(F.lit("2025-03-01"))))
    .select(
        "*",
        F.length("attr_key").alias("attr_key_length"),
        F.when(F.col("attr_key").isin(["weight_kg", "utm_source"]), "Core").otherwise("Other").alias("attr_key_group"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.max("attr_key_length").over(w).alias("attr_key_length_max_2h"),
        F.avg("attr_key_length").over(w2).alias("attr_key_avg_attr_key_length"),
    )
)

cleaned_carts = (
    enriched
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .withColumn("created_at_hour", F.hour("created_at"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("DIRECT")))
)

result = (
    cleaned_carts
    .select(
        "id",
        "created_at",
        "attr_key",
        "attr_value",
        "tag",
        "attr_key_length",
        "attr_key_length_max_2h",
        "attr_key_avg_attr_key_length",
        "created_at_hour",
        F.when(F.col("tag").rlike("^(VIP|WELCOME5)$"), "PRIORITY").otherwise("GROUP_A").alias("tag_zone"),
    )
    .orderBy("created_at_hour")
)
