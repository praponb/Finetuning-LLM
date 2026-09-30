from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").asc_nulls_first(), "id")

latest = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("status").rlike("^(RETURNED|IN_TRANSIT)$"), "CORE").otherwise("PRIORITY").alias("status_zone"),
    )
)
