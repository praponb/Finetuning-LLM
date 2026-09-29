from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("order_date").desc(), "id")

deduped = (
    df_orders
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .withColumn("week_start", F.date_trunc("WEEK", "order_date"))
    .groupingSets(
        [["customer_id", "week_start"], ["customer_id"]],
        "customer_id",
        "week_start",
    )
    .agg(
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("status") == "CANCELLED", 1)), F.lit(0)), 2).alias("discount_per_cancelled"),
        F.count_if(F.col("status") == "SHIPPED").alias("shipped_events"),
        F.grouping("customer_id").alias("grouping_id"),
    )
)
