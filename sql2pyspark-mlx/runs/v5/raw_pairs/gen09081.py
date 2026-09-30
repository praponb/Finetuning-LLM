from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")
w4 = Window.orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id")
w7 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-6, -1)

latest = (
    df_inventory
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

latest_inventory = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest_inventory
    .select(
        "*",
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("event_seq_2"),
        F.last("quantity", ignorenulls=True).over(w5).alias("last_known_quantity"),
        F.sum("quantity").over(w6).alias("product_id_sum_quantity"),
        F.avg("quantity").over(w7).alias("quantity_avg_6_prior"),
    )
)

cleaned = (
    scored
    .filter(F.col("quantity_avg_6_prior").isNotNull())
    .withColumn("event_seq_plus_dedup_rank", (F.col("event_seq") + F.col("dedup_rank")))
)

filtered = (
    cleaned
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-03-17")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "row_num",
        "dedup_rank",
        "event_seq",
        "last_known_quantity",
        "product_id_sum_quantity",
        "quantity_avg_6_prior",
        "event_seq_plus_dedup_rank",
        (F.col("last_known_quantity") + F.col("event_seq_2")).alias("last_known_quantity_plus_event_seq_2"),
    )
)

result = (
    filtered
    .withColumn("year_start", F.date_trunc("YEAR", "last_restock"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 255, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["year_start", "product_id", "quantity_tier"], ["year_start", "product_id"], ["year_start"], []],
        "year_start",
        "product_id",
        "quantity_tier",
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("quantity_avg_6_prior"), 2).alias("avg_quantity_avg_6_prior"),
        F.min("event_seq_plus_dedup_rank").alias("min_event_seq_plus_dedup_rank"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("avg_quantity_avg_6_prior").desc_nulls_first())
)
