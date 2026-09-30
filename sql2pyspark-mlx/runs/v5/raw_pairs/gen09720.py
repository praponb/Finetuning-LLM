from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

metrics = (
    df_stores
    .filter(F.col("region") == "South")
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.avg("sqft").over(w).alias("running_avg_sqft"),
        F.min("sqft").over(w).alias("running_min_sqft"),
        F.sum("sqft").over(w).alias("running_sum_sqft"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = (
    metrics
    .select("id", "city", "opened_date", "sqft", "running_min_sqft", "event_seq")
    .orderBy(F.col("opened_date").desc_nulls_first())
)
