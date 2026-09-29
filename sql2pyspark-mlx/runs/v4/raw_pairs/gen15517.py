from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").asc_nulls_first(), "id")
w2 = Window.orderBy("dedup_rank", "id")
w_part = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc_nulls_first())

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2024-04-03").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_trunc("WEEK", "last_restock").alias("last_restock_period"),
        F.when(F.col("quantity") >= 259, "High").when(F.col("quantity") >= 81, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("quantity_rank", F.rank().over(w_part))
)

result = (
    enriched
    .withColumn("activity_month", F.date_trunc("MONTH", "last_restock_period"))
    .cube("activity_month")
    .agg(
        F.max("last_restock_period").alias("last_seen"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.count("*").alias("num_rows"),
        F.grouping("activity_month").alias("gid"),
    )
    .filter(F.col("distinct_store_ids") < 1)
    .orderBy(F.col("distinct_store_ids").desc())
)
