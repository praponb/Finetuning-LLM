from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_first(), "id")

deduped = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("carrier").rlike("^(UPS|DHL)$"), "GROUP_B").when(F.col("carrier").rlike("^(FedEx|Kerry)$"), "CORE").otherwise("PRIORITY").alias("carrier_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
