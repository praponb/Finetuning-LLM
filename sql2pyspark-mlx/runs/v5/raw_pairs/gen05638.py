from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_last(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("order_status").orderBy("sqft", "id")
w_part = Window.partitionBy("order_status").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-03-30")))
    .withColumn("sqft_filled", F.coalesce("sqft", F.lit(0)))
    .withColumn("store_name_upper", F.upper("store_name"))
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "city", "region", "sqft")
)

latest = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

latest_match = (
    latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.sqft",
        F.col("c.status").alias("order_status"),
        F.col("c.order_date").alias("order_order_date"),
    )
)

ranked = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "sqft", "order_status")
)

with_windows = (
    ranked
    .withColumn("running_avg_sqft", F.avg("sqft").over(w_part))
    .withColumn("running_max_sqft", F.max("sqft").over(w_part))
    .withColumn("running_min_sqft", F.min("sqft").over(w_part))
    .withColumn("running_sum_sqft", F.sum("sqft").over(w_part))
    .withColumn("running_sqft", F.sum("sqft").over(w6))
)

result = (
    with_windows
    .select(
        "id",
        "sqft",
        "order_status",
        "running_max_sqft",
        "running_min_sqft",
        F.when(F.col("order_status").rlike("^(CANCELLED|COMPLETED)$"), "GROUP_B").when(F.col("order_status").rlike("^(PENDING|SHIPPED)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("order_status_zone"),
    )
    .orderBy(F.col("running_max_sqft").desc(), "order_status")
)
