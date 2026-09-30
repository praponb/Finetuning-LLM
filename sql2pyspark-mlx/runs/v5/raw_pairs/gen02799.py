from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.initcap(F.trim("region")).alias("region_title"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.opened_date")), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.opened_date",
        "s.sqft",
        "s.days_since_opened_date",
        "s.region_title",
        F.col("c.order_date").alias("order_order_date"),
    )
)

scored = (
    with_last
    .filter(F.col("order_order_date") > "2024-07-27")
    .withColumn("last4_max_sqft", F.max("sqft").over(w2))
    .withColumn("last4_count_sqft", F.count("sqft").over(w2))
    .withColumn("last4_avg_sqft", F.avg("sqft").over(w2))
)

filtered = (
    scored
    .filter((F.col("opened_date") >= F.lit("2023-05-05").cast("date")) & (F.col("opened_date") <= "2024-01-01"))
    .withColumn("last4_avg_sqft_rounded", F.round("last4_avg_sqft", 2))
)

with_last_stores = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.store_name", "s.last4_avg_sqft", F.col("c.quantity"))
)

result = (
    with_last_stores
    .select(
        "id",
        "store_name",
        "last4_avg_sqft",
        F.when(F.col("store_name").rlike("^(Old Town|Downtown Express)$"), "EXTENDED").otherwise("GROUP_A").alias("store_name_zone"),
    )
)
