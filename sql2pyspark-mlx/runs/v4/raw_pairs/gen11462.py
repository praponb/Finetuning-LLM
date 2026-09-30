from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc())
w2 = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_last())
w_part = Window.orderBy(F.col("cost").desc_nulls_last())
w5 = Window.partitionBy("status").orderBy(F.col("cost").asc_nulls_first())

filtered = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-11-01").cast("date"))
    .withColumn("status_upper", F.upper("status"))
)

kept_keys = (
    filtered
    .select("carrier")
    .intersect(
        df_shipments
        .filter(F.col("cost") >= 79)
        .select("carrier")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.cume_dist().over(w).alias("cost_cume_dist"),
        F.percent_rank().over(w2).alias("cost_percent_rank"),
        F.rank().over(w3).alias("cost_rank"),
        F.dense_rank().over(w_part).alias("cost_dense_rank"),
        F.percent_rank().over(w5).alias("cost_percent_rank_2"),
    )
)

filtered_shipments = (
    with_windows
    .filter((F.col("shipped_date") >= F.lit("2023-12-02").cast("date")) & (F.col("cost") < 53))
    .select(
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "cost_percent_rank",
        "cost_rank",
        "cost_dense_rank",
        "cost_percent_rank_2",
        F.when(F.col("cost_cume_dist") >= 0.65, "High").when(F.col("cost_cume_dist") >= 0.13, "Medium").otherwise("Low").alias("cost_cume_dist_level"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

result = (
    filtered_shipments
    .select(
        "delivered_date",
        "status",
        "cost_percent_rank",
        "cost_rank",
        "cost_dense_rank",
        "cost_percent_rank_2",
        "cost_cume_dist_level",
        "resolved_carrier",
    )
)
