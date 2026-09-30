from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("paid_at", "id")

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-11-02").cast("date")) & F.col("order_id").isNull())
    .withColumn("status_length", F.length("status"))
    .withColumn("status_upper", F.upper("status"))
)

result = (
    valid
    .select(
        "*",
        F.avg("status_length").over(w).alias("status_length_avg_2"),
        F.lead("status_length", 2).over(w2).alias("next_status_length"),
    )
)
