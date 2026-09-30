from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device", "country").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("device", "country").orderBy("started_at_period", "id")

valid = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.date_trunc("month", "started_at").alias("started_at_period"),
    )
)

result = (
    valid
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("event_seq_2", F.row_number().over(w3))
)
