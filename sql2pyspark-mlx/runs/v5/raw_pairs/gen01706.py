from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key")
w2 = Window.orderBy(F.col("created_at").desc(), "id", "attr_key")
w_grp = Window.partitionBy("attr_key").orderBy(F.col("created_at").desc(), "id", "attr_key")

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "value"),
    )
)

deduped = (
    exploded
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "attr_key",
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w_grp).alias("row_seq_2"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

cleaned = (
    enriched
    .filter(F.col("attr_key").isNotNull() & (F.col("attr_key").eqNullSafe(F.lit("weight_kg"))))
    .select(
        "id",
        "attr_key",
        "row_seq",
        "row_seq_2",
        (F.col("row_seq") + F.col("seq_no")).alias("row_seq_plus_seq_no"),
    )
)

result = (
    cleaned
    .cube(
        F.when(F.col("row_seq_plus_seq_no") >= 69, "PREMIUM").otherwise("STANDARD").alias("row_seq_plus_seq_no_tier"),
        F.when(F.col("row_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count(F.when(F.col("attr_key") == "device", 1)).alias("device_rows"),
        F.sum("row_seq_plus_seq_no").alias("total_row_seq_plus_seq_no"),
    )
    .orderBy("row_seq_tier")
)
