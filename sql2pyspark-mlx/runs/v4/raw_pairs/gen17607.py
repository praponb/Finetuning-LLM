from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("store_id_avg_days_since_last_restock", "id").rowsBetween(-3, -1)
w4 = Window.orderBy("last_restock_period", "id")
w_part = Window.partitionBy("store_id").orderBy("days_since_last_restock_max_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("days_since_last_restock_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-07-16").cast("date"))
    .select(
        "id",
        "store_id",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
        F.date_trunc("month", "last_restock").alias("last_restock_period"),
    )
)

scored = (
    base
    .select(
        "*",
        F.avg("days_since_last_restock").over(w).alias("store_id_avg_days_since_last_restock"),
        F.max("days_since_last_restock").over(w2).alias("days_since_last_restock_max_7d"),
    )
)

enriched = (
    scored
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.store_name"))
)

enriched_inventory = (
    enriched
    .withColumn("days_since_last_restock_avg_3_prior", F.avg("days_since_last_restock").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

scored_inventory = (
    enriched_inventory
    .select(
        "*",
        F.first("days_since_last_restock_avg_3_prior", ignorenulls=True).over(w_part).alias("first_known_days_since_last_restock_avg_3_prior"),
        F.first("store_id_avg_days_since_last_restock", ignorenulls=True).over(w6).alias("first_known_store_id_avg_days_since_last_restock"),
    )
)

result = (
    scored_inventory
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "days_since_last_restock",
        "last_restock_period",
        "store_id_avg_days_since_last_restock",
        "days_since_last_restock_max_7d",
        "store_name",
        "first_known_days_since_last_restock_avg_3_prior",
        F.when(F.col("store_name").rlike("^(Downtown Express|Old Town)$"), "EXTENDED").when(F.col("store_name").rlike("^(Siam Flagship|Airport Kiosk)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("store_name_zone"),
        F.col("min_order_shipping_fee"),
    )
    .orderBy(F.col("id").asc_nulls_first(), F.col("min_order_shipping_fee").desc())
)
