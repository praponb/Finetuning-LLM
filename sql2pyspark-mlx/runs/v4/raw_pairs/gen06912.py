from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").asc_nulls_last(), "id")

deduped = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .withColumn("activity_month", F.date_trunc("MONTH", "order_date"))
    .groupingSets(
        [["activity_month", "store_id"], ["activity_month"], []],
        "activity_month",
        "store_id",
    )
    .agg(
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_rows"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .orderBy(F.col("completed_rows").desc())
)
