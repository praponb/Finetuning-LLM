from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("region").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("region").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("region").orderBy("rn", "id")
w6 = Window.orderBy("quantity", "id")

latest_match = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

metrics = (
    latest_match
    .withColumn("last_known_region", F.last("region", ignorenulls=True).over(w2))
    .withColumn("quantity_avg_3", F.avg("quantity").over(w_ordered))
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("quantity_bucket", F.ntile(10).over(w6))
)

kept = (
    scored
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.revenue") <= 1300), "left_anti")
)

valid = (
    kept
    .filter((F.col("opened_date") >= F.to_date(F.lit("2024-04-07"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "inventory_last_restock",
        "quantity",
        "last_known_region",
        "quantity_avg_3",
        "quantity_bucket",
        F.round("quantity_bucket", 1).alias("quantity_bucket_rounded"),
    )
)

result = (
    valid
    .groupBy("region")
    .agg(
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.avg("quantity_avg_3").alias("avg_quantity_avg_3"),
    )
    .orderBy(F.col("sqft_bit_and").desc())
)
