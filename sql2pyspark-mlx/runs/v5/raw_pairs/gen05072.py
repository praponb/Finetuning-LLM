from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date_period", "id")

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("DHL"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        F.concat("carrier", F.lit(", "), "status").alias("carrier_status"),
        F.date_trunc("MONTH", "shipped_date").alias("shipped_date_period"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "cost", "status")
)
