from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_hist = Window.orderBy(F.col("created_at_dayofweek").desc_nulls_first())

flattened = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("value"))

cleaned = (
    flattened
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & F.col("value").isNotNull())
    .select(
        "*",
        F.when(F.col("value").isin(["WELCOME5", "BUNDLE"]), "Tier 1").otherwise("Other").alias("value_group"),
        F.coalesce("value", F.lit("DIRECT")).alias("value_filled"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("value").isin(["BUNDLE", "WELCOME5"]))
    .select(
        "id",
        "created_at",
        "value_group",
        "value_filled",
        F.dayofweek("created_at").alias("created_at_dayofweek"),
        F.date_format("created_at", "yyyy-MM").alias("created_at_label"),
        F.coalesce("value", F.lit("DIRECT")).alias("value_filled_2"),
    )
)

filtered = (
    cleaned_carts
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-02 14:00:00"))) & F.hour("created_at").between(10, 17))
    .withColumn("created_at_dayofweek_str", F.col("created_at_dayofweek").cast("string"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.avg("created_at_dayofweek").over(w).alias("created_at_dayofweek_avg_2h"),
        F.rank().over(w_hist).alias("created_at_dayofweek_rank"),
    )
)

filtered_carts = (
    with_windows
    .filter(F.col("created_at_dayofweek").between(22, 48))
    .select("*", F.hour("created_at").alias("created_at_hour"))
)

filtered_carts_2 = (
    filtered_carts
    .filter(F.col("created_at") > F.lit("2025-03-02 14:00:00").cast("timestamp"))
    .withColumn("created_at_dayofweek_rank_per_created_at_dayofweek_avg_2h", (F.col("created_at_dayofweek_rank") / F.nullif("created_at_dayofweek_avg_2h", F.lit(0))))
    .withColumn("created_at_dayofweek_avg_2h_rounded", F.round("created_at_dayofweek_avg_2h", 1))
)

result = (
    filtered_carts_2
    .select(
        "id",
        "created_at",
        "value_group",
        "value_filled",
        "created_at_dayofweek_avg_2h",
        "created_at_dayofweek_rank",
        "created_at_hour",
        "created_at_dayofweek_rank_per_created_at_dayofweek_avg_2h",
    )
)
