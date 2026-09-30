from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("shipped_date").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("shipped_date_plus_months").asc_nulls_first(), "id")
w3 = Window.partitionBy("carrier").orderBy(F.col("shipped_date_plus_months").desc_nulls_first(), "id")

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("status").isin(["LOST", "DELIVERED"]), "Group A").otherwise("Other").alias("status_group"),
        F.add_months("shipped_date", 1).alias("shipped_date_plus_months"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "carrier", "status", "status_group", "shipped_date_plus_months")
)

deduped = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "carrier", "status_group", "shipped_date_plus_months")
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "carrier", "shipped_date_plus_months")
)

result = (
    ranked
    .select(
        "id",
        "carrier",
        "shipped_date_plus_months",
        F.when(F.col("carrier").rlike("^(DHL|Flash)$"), "PRIORITY").otherwise("GROUP_B").alias("carrier_zone"),
    )
)
