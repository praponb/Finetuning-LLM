from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc_nulls_first())
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")

metrics = (
    df_shipments
    .withColumn("cost_cume_dist", F.cume_dist().over(w))
    .withColumn("cost_bucket", F.ntile(10).over(w2))
)

result = (
    metrics
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        "cost_cume_dist",
        "cost_bucket",
        F.when(F.col("carrier").rlike("^(DHL|FedEx)$"), "PRIORITY").when(F.col("carrier").rlike("^(Kerry|Flash)$"), "CORE").otherwise("GROUP_A").alias("carrier_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
