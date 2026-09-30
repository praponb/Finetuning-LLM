from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("last_restock_period", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").desc(), "id")
w4 = Window.orderBy(F.col("last_restock").desc(), "id")
w5 = Window.orderBy("last_restock", "id")
w6 = Window.orderBy("event_seq_2", "id")

cleaned = (
    df_inventory
    .filter(F.col("quantity") == 105)
    .select(
        "id",
        "product_id",
        "last_restock",
        F.date_trunc("week", "last_restock").alias("last_restock_period"),
    )
)

enriched = (
    cleaned
    .filter(F.col("product_id").isin([1, 4, 9]))
    .select(
        "id",
        "last_restock_period",
        F.row_number().over(w_recent).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("row_seq_2"),
    )
)

filtered = (
    enriched
    .filter(F.col("row_seq") <= 5)
    .select(
        "id",
        "last_restock_period",
        "event_seq",
        "event_seq_2",
        F.round((F.col("event_seq_2") * 1.15), 2).alias("adjusted_event_seq_2"),
    )
)

enriched_inventory = (
    filtered
    .select(
        "id",
        "event_seq_2",
        "adjusted_event_seq_2",
        F.lag("event_seq_2", 2, 0).over(w6).alias("prev_event_seq_2"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

base = (
    enriched_inventory
    .filter(F.col("row_seq") <= 3)
    .select(
        "*",
        F.when(F.col("adjusted_event_seq_2") >= 60, "High").when(F.col("adjusted_event_seq_2") >= 33, "Medium").otherwise("Low").alias("adjusted_event_seq_2_tier"),
        F.when(F.col("prev_event_seq_2") >= 7, "High").when(F.col("prev_event_seq_2") >= 4, "Medium").otherwise("Low").alias("prev_event_seq_2_level"),
    )
)

metrics = (
    base
    .select(
        "id",
        "adjusted_event_seq_2",
        "adjusted_event_seq_2_tier",
        "prev_event_seq_2_level",
        F.lead("row_seq").over(w6).alias("next_row_seq"),
        F.lag("row_seq").over(w6).alias("prev_row_seq"),
        F.lag("event_seq_2").over(w6).alias("prev_event_seq_2_2"),
        F.lead("event_seq_2").over(w6).alias("next_event_seq_2"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

valid = (
    metrics
    .filter(F.col("prev_row_seq") > 3)
    .select(
        "id",
        "adjusted_event_seq_2_tier",
        "prev_event_seq_2_level",
        "next_row_seq",
        "prev_row_seq",
        "prev_event_seq_2_2",
        F.round("next_row_seq", 0).alias("next_row_seq_rounded"),
    )
)

totals = (
    valid
    .groupBy(
        F.when(F.col("prev_event_seq_2_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("prev_event_seq_2_2_tier"),
        F.when(F.col("next_row_seq") >= 8, "HIGH").otherwise("LOW").alias("next_row_seq_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("prev_row_seq"), 2).alias("avg_prev_row_seq"),
        F.count("*").alias("cnt_2"),
    )
    .filter(F.col("avg_prev_row_seq") < 1000000)
)

result = totals.select("prev_event_seq_2_2_tier", "next_row_seq_tier", "cnt_2")
