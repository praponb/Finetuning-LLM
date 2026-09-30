from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status", "carrier").orderBy("cost", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("shipped_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id", "status").orderBy("shipped_date", "id").rowsBetween(-5, Window.currentRow)

metrics = (
    df_shipments
    .select(
        "*",
        F.max("cost").over(w).alias("status_max_cost"),
        F.avg("cost").over(w2).alias("cost_avg_3"),
        F.avg("cost").over(w3).alias("cost_avg_6_prior"),
        F.count("cost").over(w4).alias("order_id_count_cost"),
        F.avg("cost").over(w5).alias("cost_avg_5"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("MONTH", "delivered_date").alias("activity_month"),
        F.when(F.col("cost_avg_3") >= 106, "HIGH").otherwise("LOW").alias("cost_avg_3_tier"),
        "order_id",
    )
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)), F.lit(0)), 2).alias("cost_per_in_transit"),
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("order_id_count_cost")).otherwise(0)).alias("kerry_order_id_count_cost"),
        F.max("order_id_count_cost").alias("max_order_id_count_cost"),
        F.avg(F.when(F.col("carrier") == "DHL", F.col("cost_avg_6_prior"))).alias("dhl_avg_cost_avg_6_prior"),
    )
    .orderBy(F.col("cost_per_in_transit").asc_nulls_first())
)
