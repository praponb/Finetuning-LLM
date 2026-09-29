from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("num_rows").desc())

deduped = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy(
        "status",
        F.when(F.col("cost") >= 88, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        "order_id",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
    )
    .filter(F.col("dhl_cost") > 0)
    .withColumn("num_rows_percent_rank", F.percent_rank().over(w2))
)
