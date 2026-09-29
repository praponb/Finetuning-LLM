from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date_month_end").desc_nulls_last(), "id")

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-06-23").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "carrier", "shipped_date")
)
