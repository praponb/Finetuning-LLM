from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Safari", "Edge"]))
    .select(
        "*",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.last("country", ignorenulls=True).over(w).alias("last_known_country_2"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country_3"),
    )
)

result = (
    enriched
    .groupBy("country", F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.min("started_at").alias("started_at"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .filter(F.col("distinct_customer_ids") > 1)
)
