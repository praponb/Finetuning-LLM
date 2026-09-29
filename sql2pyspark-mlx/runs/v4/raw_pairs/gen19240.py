from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")

ranked = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.when(F.col("carrier").rlike("^(Flash|DHL)$"), "GROUP_A").otherwise("GROUP_B").alias("carrier_zone"),
    )
    .orderBy(F.col("carrier_zone").desc())
)
