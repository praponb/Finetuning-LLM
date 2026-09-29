from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity_whole", "id")
w_part = Window.orderBy(F.col("last_restock_minus_60d").desc(), "id")
w3 = Window.orderBy("row_seq", "id")
w4 = Window.orderBy("quantity_whole", "id").rowsBetween(-6, -1)
w5 = Window.orderBy(F.col("row_seq").desc_nulls_last())
w6 = Window.orderBy("quantity_per_row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("last_restock").between("2024-02-07", "2024-09-06"))
    .select(
        "*",
        F.date_sub("last_restock", 60).alias("last_restock_minus_60d"),
        F.floor("quantity").alias("quantity_whole"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "quantity",
        "quantity_whole",
        F.ntile(10).over(w).alias("quantity_whole_bucket"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

filtered = (
    enriched
    .filter(F.col("quantity_whole_bucket") >= 4)
    .select(
        "id",
        "quantity",
        "quantity_whole",
        "quantity_whole_bucket",
        "row_seq",
        F.when(F.col("quantity_whole") >= 61, "High").when(F.col("quantity_whole") >= 18, "Medium").otherwise("Low").alias("quantity_whole_tier"),
        F.round((F.col("quantity") / F.col("row_seq")), 2).alias("quantity_per_row_seq"),
    )
)

metrics = (
    filtered
    .withColumn("next_quantity_per_row_seq", F.lead("quantity_per_row_seq", 2).over(w3))
    .withColumn("row_seq_avg_6_prior", F.avg("row_seq").over(w4))
    .withColumn("row_seq_rank", F.rank().over(w5))
    .withColumn("cumulative_quantity_whole", F.sum("quantity_whole").over(w6))
)

valid = (
    metrics
    .filter(F.col("next_quantity_per_row_seq") < 78)
    .withColumn("quantity_per_row_seq_int", F.col("quantity_per_row_seq").cast("int"))
)

daily = (
    valid
    .groupBy(
        F.when(F.col("quantity_whole") >= 75, "HIGH").otherwise("LOW").alias("quantity_whole_tier"),
        F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        F.avg("quantity_per_row_seq").alias("avg_quantity_per_row_seq"),
        F.round(F.avg("quantity_per_row_seq"), 2).alias("avg_quantity_per_row_seq_2"),
    )
)

result = (
    daily
    .select(
        "quantity_whole_tier",
        "row_seq_tier",
        "avg_quantity_per_row_seq",
        "avg_quantity_per_row_seq_2",
    )
    .orderBy("quantity_whole_tier", "avg_quantity_per_row_seq_2")
)
