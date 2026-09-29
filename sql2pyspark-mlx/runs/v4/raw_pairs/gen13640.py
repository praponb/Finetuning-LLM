from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy("city").orderBy(F.col("opened_date").desc_nulls_last(), "id")

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

with_windows = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("next_days_since_opened_date", F.lead("days_since_opened_date", 1).over(w))
)

latest = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "store_name",
        "opened_date",
        "next_days_since_opened_date",
        "latest_rank",
        F.when(F.col("store_name").rlike("^(Mall Central|Old Town)$"), "PRIORITY").otherwise("EXTENDED").alias("store_name_zone"),
    )
    .orderBy(F.col("next_days_since_opened_date").desc_nulls_first())
)
