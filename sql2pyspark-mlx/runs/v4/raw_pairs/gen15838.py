from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at_quarter").desc())
w3 = Window.partitionBy("customer_id").orderBy(F.col("created_at_quarter").asc_nulls_first())
w4 = Window.partitionBy("k").orderBy(F.col("created_at_quarter").desc())
w5 = Window.partitionBy("customer_id").orderBy(F.col("created_at_quarter_percent_rank").asc_nulls_first())
w6 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

cleaned = (
    flattened
    .filter(F.col("created_at") >= F.to_date(F.lit("2025-03-02")))
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        F.quarter("created_at").alias("created_at_quarter"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.min("created_at_quarter").over(w_grp).alias("customer_id_min_created_at_quarter"),
        F.percent_rank().over(w2).alias("created_at_quarter_percent_rank"),
        F.sum("created_at_quarter").over(w_grp).alias("customer_id_sum_created_at_quarter"),
        F.dense_rank().over(w3).alias("created_at_quarter_dense_rank"),
    )
)

with_windows = (
    metrics
    .withColumn("created_at_quarter_rank", F.rank().over(w4))
    .withColumn("created_at_quarter_percent_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("customer_id_min_created_at_quarter_avg_2h", F.avg("customer_id_min_created_at_quarter").over(w6))
)

result = (
    with_windows
    .select(
        "customer_id",
        "created_at",
        "k",
        "created_at_quarter",
        "customer_id_min_created_at_quarter",
        "created_at_quarter_percent_rank",
        "customer_id_sum_created_at_quarter",
        "created_at_quarter_rank",
        "created_at_quarter_percent_rank_percent_rank",
        F.when(F.col("k").rlike("^(weight_kg|gift)$"), "GROUP_A").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy(F.col("customer_id_min_created_at_quarter").desc_nulls_last())
)
