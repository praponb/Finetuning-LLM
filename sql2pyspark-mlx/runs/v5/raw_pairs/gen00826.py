from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("code").orderBy(F.col("created_at_dayofweek").desc_nulls_last())
w4 = Window.partitionBy("code").orderBy(F.col("created_at_dayofweek").desc())

lines = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("code"))

valid = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.col("code").isin(["VIP", "SPRING10", "BUNDLE"]))
    .select(
        "*",
        F.dayofweek("created_at").alias("created_at_dayofweek"),
        F.to_date("created_at").alias("created_at_date"),
        F.coalesce("code", F.lit("UNKNOWN")).alias("code_clean"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "created_at",
        F.max("created_at_dayofweek").over(w).alias("created_at_dayofweek_max_15m"),
        F.avg("created_at_dayofweek").over(w2).alias("created_at_dayofweek_avg_30m"),
        F.rank().over(w3).alias("created_at_dayofweek_rank"),
        F.percent_rank().over(w4).alias("created_at_dayofweek_percent_rank"),
    )
)

valid_carts = (
    with_windows
    .filter(F.col("created_at_dayofweek_avg_30m").isNull())
    .withColumn("created_at_dayofweek_percent_rank_band", F.when(F.col("created_at_dayofweek_percent_rank") >= 0.65, "High").when(F.col("created_at_dayofweek_percent_rank") >= 0.39, "Medium").otherwise("Low"))
)

base = (
    valid_carts
    .filter(F.col("created_at") >= "2025-10-01")
    .withColumn("created_at_dayofweek_max_15m_minus_created_at_dayofweek_percent_rank", (F.col("created_at_dayofweek_max_15m") - F.col("created_at_dayofweek_percent_rank")))
)

result = (
    base
    .select(
        "id",
        "created_at_dayofweek_max_15m",
        "created_at_dayofweek_avg_30m",
        "created_at_dayofweek_rank",
        "created_at_dayofweek_percent_rank",
        "created_at_dayofweek_percent_rank_band",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
