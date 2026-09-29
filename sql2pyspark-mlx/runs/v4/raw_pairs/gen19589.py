from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w3 = Window.partitionBy("product_id").orderBy("event_seq", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("product_id").orderBy("event_seq", "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-11-24").cast("date"))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.when(F.col("quantity") >= 218, "High").when(F.col("quantity") >= 107, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

enriched = (
    valid
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("row_seq_2", F.row_number().over(w2))
    .withColumn("event_seq_2", F.row_number().over(w2))
    .withColumn("event_seq_3", F.row_number().over(w))
)

enriched_inventory = (
    enriched
    .select(
        "*",
        F.avg("row_seq_2").over(w3).alias("row_seq_2_avg_6_prior"),
        F.lag("event_seq", 1).over(w4).alias("prev_event_seq"),
    )
)

result = (
    enriched_inventory
    .groupBy("product_id")
    .agg(
        F.avg("event_seq_3").alias("avg_event_seq_3"),
        F.percentile_approx("row_seq_2", 0.5).alias("row_seq_2_p50"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("event_seq_2", [0.25, 0.5, 0.75]).alias("event_seq_2_quartiles"),
    )
    .orderBy(F.col("avg_event_seq_3").asc_nulls_last())
)
