from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc_nulls_first(), "id")

valid = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-05-16").cast("date")) & F.col("cost").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "status",
        F.concat_ws("/", "status", "carrier").alias("status_carrier_key"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    ranked
    .select("order_id", "status")
    .intersect(
        df_shipments
        .filter(F.col("cost").between(54, 55))
        .select("order_id", "status")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("status")
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.size(F.collect_set("status")).alias("num_status_2"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
    )
    .filter(F.col("num_status_2") < 1)
    .orderBy("num_status")
)
