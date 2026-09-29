from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w3 = Window.orderBy("duration_sec", "id")
w4 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_first())
w5 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_last())
w6 = Window.partitionBy("page").orderBy(F.col("prev_duration_sec").desc_nulls_last(), "id")

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-05-29 17:00:00").cast("timestamp")) & (F.to_date("event_time") < "2025-08-15"))
    .withColumn("page_event_type_key", F.concat_ws("/", "page", "event_type"))
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

filtered = (
    latest
    .filter(F.to_date("event_time") < "2023-12-01")
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "page_event_type_key",
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.lag("duration_sec", 1).over(w2).alias("prev_duration_sec"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

filtered_web_events = (
    enriched
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "page",
        "duration_sec",
        "page_event_type_key",
        "duration_sec_num",
        "prev_duration_sec",
        "row_seq",
        F.round((F.col("prev_duration_sec") / F.col("row_seq")), 2).alias("prev_duration_sec_per_row_seq"),
        F.when(F.col("page").isin(["/search", "/product"]), "Priority").otherwise("Other").alias("page_group"),
    )
)

enriched_web_events = (
    filtered_web_events
    .withColumn("duration_sec_dense_rank", F.dense_rank().over(w4))
    .withColumn("duration_sec_dense_rank_2", F.dense_rank().over(w5))
)

deduped = (
    enriched_web_events
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("row_seq") >= 8, "HIGH").otherwise("LOW").alias("row_seq_tier"),
    )
    .agg(
        F.size(F.collect_set("page")).alias("num_pages"),
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec_dense_rank")).otherwise(0)).alias("product_duration_sec_dense_rank"),
    )
)
