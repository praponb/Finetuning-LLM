from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

deduped = (
    df_sessions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        F.date_trunc("month", "started_at").alias("activity_month"),
        F.date_trunc("quarter", "started_at").alias("quarter_start"),
    )
    .agg(
        F.count_if(F.col("browser") == "Edge").alias("edge_events"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.sort_array(F.collect_set("browser")).alias("browser_list_2"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
