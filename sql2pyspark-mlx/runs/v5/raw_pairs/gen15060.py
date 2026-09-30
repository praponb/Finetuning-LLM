from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_last())
w_hist = Window.orderBy("quantity", "id")
w_ordered = Window.partitionBy("product_id").orderBy("last_restock", "id")
w4 = Window.orderBy("days_since_last_restock", "id")
w5 = Window.orderBy("prev_days_since_last_restock", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-04-01"))) & (F.col("quantity") < 181))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w_part).alias("quantity_cume_dist"),
        F.ntile(5).over(w_hist).alias("quantity_bucket"),
        F.lag("days_since_last_restock", 2).over(w_hist).alias("prev_days_since_last_restock"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.opened_date").between("2023-11-01", "2024-11-01"))
    .select(
        "src.id",
        "src.store_id",
        "src.quantity",
        "src.last_restock",
        "src.last_restock_label",
        "src.days_since_last_restock",
        "src.quantity_cume_dist",
        "src.prev_days_since_last_restock",
        "src.event_seq",
        F.col("b.opened_date"),
        F.col("b.store_name"),
    )
)

with_dims = (
    joined
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.store_id",
        "src.last_restock",
        "src.last_restock_label",
        "src.days_since_last_restock",
        "src.prev_days_since_last_restock",
        "src.opened_date",
        F.col("d.city"),
    )
)

scored = (
    with_dims
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("prev_days_since_last_restock_avg_6", F.avg("prev_days_since_last_restock").over(w5))
)

result = (
    scored
    .groupBy("store_id")
    .pivot("city", ["Berlin", "Tokyo"])
    .agg(
        F.max("prev_days_since_last_restock_avg_6").alias("total"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
