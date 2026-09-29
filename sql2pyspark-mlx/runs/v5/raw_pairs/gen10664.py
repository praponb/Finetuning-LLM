from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc())

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "shipped_date",
        "cost",
        "status",
        F.date_trunc("week", "delivered_date").alias("delivered_date_period"),
    )
)

kept_keys = (
    base
    .select("status", "id")
    .intersectAll(
        df_shipments
        .filter(F.col("carrier") != "Flash")
        .select("status", "id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("shipped_date") > F.lit("2023-06-20").cast("date"))
    .withColumn("days_since_delivered_date_period", F.datediff(F.current_date(), "delivered_date_period"))
    .withColumn("shipped_date_plus_months", F.add_months("shipped_date", 6))
)

with_windows = (
    valid
    .filter(F.hour("delivered_date_period").between(9, 16))
    .withColumn("cost_dense_rank", F.dense_rank().over(w))
)

result = (
    with_windows
    .select(
        "id",
        "shipped_date",
        "cost",
        "status",
        "days_since_delivered_date_period",
        "shipped_date_plus_months",
        "cost_dense_rank",
    )
    .orderBy(F.col("days_since_delivered_date_period").desc())
)
