from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipped_date_quarter").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy("shipped_date_quarter", "id")
w3 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(-5, Window.currentRow)

valid = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-04-21"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.quarter("shipped_date").alias("shipped_date_quarter"),
    )
)

result = (
    valid
    .withColumn("shipped_date_quarter_bucket", F.ntile(3).over(w))
    .withColumn("prev_shipped_date_quarter", F.lag("shipped_date_quarter", 1, 0).over(w2))
    .withColumn("shipped_date_quarter_avg_5", F.avg("shipped_date_quarter").over(w3))
)
