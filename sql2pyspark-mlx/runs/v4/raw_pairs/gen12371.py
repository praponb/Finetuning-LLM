from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id", "country").orderBy("started_at", "id")
w4 = Window.partitionBy("browser").orderBy(F.col("seq_no").desc(), "id")
w5 = Window.partitionBy("country", "browser").orderBy("seq_no", "id")
w6 = Window.partitionBy("customer_id", "country").orderBy("seq_no", "id")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

filtered = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-04-25").cast("date"))
    .select(
        "*",
        F.concat_ws("-", "device", "country").alias("device_country_key"),
        F.when(F.col("country").isin(["TH", "SG"]), "Core").otherwise("Other").alias("country_group"),
    )
)

scored = (
    filtered
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

metrics = (
    scored
    .select(
        "*",
        F.row_number().over(w4).alias("row_seq"),
        F.lead("seq_no", 1).over(w5).alias("next_seq_no"),
        F.lead("seq_no", 1).over(w6).alias("next_seq_no_2"),
    )
)

result = (
    metrics
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "started_at",
        "device_country_key",
        "country_group",
        "first_known_country",
        "seq_no",
        "event_seq",
        "row_seq",
        F.col("avg_order_discount"),
    )
    .orderBy(F.col("row_seq").asc_nulls_first())
)
