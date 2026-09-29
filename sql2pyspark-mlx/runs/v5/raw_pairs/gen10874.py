from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("attr_key").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

filtered = (
    flattened
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("attr_key").isNotNull())
    .select("*", F.substring("attr_key", 1, 2).alias("attr_key_prefix"))
)

filtered_carts = (
    filtered
    .filter(F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 16:00:00")))
    .select(
        "*",
        F.when(F.col("attr_key").isin(["gift", "device"]), "Priority").otherwise("Other").alias("attr_key_group"),
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_clean"),
    )
)

scored = (
    filtered_carts
    .filter(F.col("days_since_created_at") == 25)
    .select(
        "*",
        F.count("days_since_created_at").over(w).alias("days_since_created_at_count_2h"),
        F.sum("days_since_created_at").over(w2).alias("days_since_created_at_sum_2h"),
    )
)

cleaned = (
    scored
    .filter(F.col("created_at") >= F.to_date(F.lit("2025-03-02")))
    .select(
        "created_at",
        "value",
        "days_since_created_at",
        "days_since_created_at_count_2h",
        F.dayofweek("created_at").alias("created_at_dayofweek"),
        F.coalesce("attr_key", F.lit("N/A")).alias("resolved_attr_key"),
    )
)

result = (
    cleaned
    .select("created_at", "value", "days_since_created_at", "created_at_dayofweek")
)
