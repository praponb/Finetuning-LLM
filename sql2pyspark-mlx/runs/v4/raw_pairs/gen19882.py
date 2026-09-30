from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_first(), "id")

deduped = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
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
        "rn",
        F.when(F.col("status").rlike("^(RETURNED|LOST)$"), "CORE").when(F.col("status").rlike("^(IN_TRANSIT|DELIVERED)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("status_zone"),
    )
)
