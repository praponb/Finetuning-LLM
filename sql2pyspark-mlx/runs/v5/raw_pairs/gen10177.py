from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

metrics = (
    df_shipments
    .withColumn("last7_max_cost", F.max("cost").over(w))
    .withColumn("last7_count_cost", F.count("cost").over(w))
    .withColumn("last7_min_cost", F.min("cost").over(w))
    .withColumn("cost_bucket", F.ntile(4).over(w2))
)

result = (
    metrics
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "last7_count_cost",
        "last7_min_cost",
        F.when(F.col("carrier").rlike("^(UPS|Flash)$"), "CORE").when(F.col("carrier").rlike("^(DHL|FedEx)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("carrier_zone"),
    )
    .orderBy(F.col("status").asc_nulls_last(), "delivered_date")
)
