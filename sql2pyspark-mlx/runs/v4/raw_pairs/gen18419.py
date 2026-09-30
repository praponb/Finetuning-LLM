from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("created_at", "id")
w2 = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        F.col("attributes")["device"].alias("device"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
    )
)

result = (
    unpacked
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("first_known_device_or_default", F.first("device_or_default", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w_ordered))
)
