from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").asc_nulls_last(), "id")
w2 = Window.orderBy("method_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("paid_at", "id")

base = (
    df_payments
    .filter(F.hour("paid_at").between(7, 14))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.length("method").alias("method_length"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

base_payments = (
    base
    .filter(F.col("method").isNotNull())
    .withColumn("method_length_level", F.when(F.col("method_length") >= 86, "High").when(F.col("method_length") >= 18, "Medium").otherwise("Low"))
    .withColumn("method_filled_2", F.coalesce("method", F.lit("DIRECT")))
)

latest = (
    base_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined = (
    latest
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.amount"), F.col("b.discount"))
)

cleaned = (
    joined
    .filter((F.col("paid_at") > F.lit("2023-06-05").cast("date")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "method_length",
        "method_filled",
        "method_length_level",
        "method_filled_2",
        "discount",
        F.concat_ws(",", "method", "status").alias("method_status_key"),
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled_3"),
    )
)

result = (
    cleaned
    .withColumn("first_known_method_length", F.first("method_length", ignorenulls=True).over(w2))
    .withColumn("next_method_length", F.lead("method_length", 1).over(w3))
)
