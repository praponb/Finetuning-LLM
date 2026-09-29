from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("duration_min").desc())

valid = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2024-02-18").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.when(F.col("method").isin(["bank_transfer", "cash"]), "Priority").otherwise("Other").alias("method_group"),
    )
)

enriched = (
    valid
    .withColumn("first_known_method", F.first("method", ignorenulls=True).over(w))
    .withColumn("first_known_method_2", F.first("method", ignorenulls=True).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.discount"), F.col("o.order_date"))
)

result = (
    with_dims
    .groupBy("order_id", "seq_no", F.date_trunc("YEAR", "paid_at").alias("year_start"))
    .agg(
        F.max("paid_at").alias("last_seen"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("method") == "card", 1)), F.lit(0)), 2).alias("discount_per_card"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
    )
    .withColumn("duration_min_percent_rank", F.percent_rank().over(w4))
    .orderBy(F.col("order_id").asc_nulls_last())
)
