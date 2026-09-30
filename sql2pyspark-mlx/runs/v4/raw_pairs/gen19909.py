from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc_nulls_first(), "id")

valid = (
    df_shipments
    .filter(F.col("delivered_date") > F.lit("2023-08-11").cast("date"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.when(F.col("status") == "DELIVERED", 1).otherwise(0).alias("is_delivered"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
