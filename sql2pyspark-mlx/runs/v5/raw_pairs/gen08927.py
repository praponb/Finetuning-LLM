from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("created_at", "id", "k")
w_part = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("created_at").desc(), "id", "k")
w4 = Window.partitionBy("k").orderBy(F.col("seq_no").desc_nulls_last())
w5 = Window.partitionBy("k").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.partitionBy("k").orderBy("seq_no_2", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("k")
w8 = Window.partitionBy("k").orderBy(F.col("seq_no_2_count_1h").desc())
w9 = Window.partitionBy("k").orderBy(F.col("seq_no_cume_dist").desc_nulls_first())
w_ordered = Window.partitionBy("k").orderBy("last_known_seq_no_2", "id", "k")

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

enriched = (
    lines
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "id",
        "created_at",
        "k",
        F.row_number().over(w).alias("seq_no"),
        F.first("k", ignorenulls=True).over(w_part).alias("first_known_k"),
        F.last("k", ignorenulls=True).over(w_part).alias("last_known_k"),
        F.row_number().over(w3).alias("seq_no_2"),
        F.last("k", ignorenulls=True).over(w_part).alias("last_known_k_2"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "k",
        "seq_no",
        "first_known_k",
        "last_known_k_2",
        F.cume_dist().over(w4).alias("seq_no_cume_dist"),
        F.count("seq_no_2").over(w5).alias("seq_no_2_count_1h"),
        F.last("seq_no_2", ignorenulls=True).over(w6).alias("last_known_seq_no_2"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "k",
        "last_known_k_2",
        "seq_no_cume_dist",
        "seq_no_2_count_1h",
        F.round((F.col("seq_no") / F.sum("seq_no").over(w7)), 4).alias("seq_no_share"),
        F.cume_dist().over(w8).alias("seq_no_2_count_1h_cume_dist"),
        F.cume_dist().over(w9).alias("seq_no_cume_dist_cume_dist"),
        F.ntile(4).over(w_ordered).alias("last_known_seq_no_2_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("seq_no_share") >= 0.81, "LARGE").otherwise("SMALL").alias("seq_no_share_tier"),
        "k",
    )
    .agg(
        F.count_if(F.col("k") == "weight_kg").alias("weight_kg_events"),
        F.countDistinct("k").alias("distinct_ks"),
        F.percentile_approx("seq_no_2_count_1h", [0.25, 0.5, 0.75]).alias("seq_no_2_count_1h_quartiles"),
    )
    .orderBy(F.col("weight_kg_events").asc_nulls_last())
)
