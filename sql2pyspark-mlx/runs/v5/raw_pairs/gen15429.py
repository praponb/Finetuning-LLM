from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status", "order_id").orderBy("shipped_date", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .withColumn("clean_carrier", F.trim("carrier"))
)

result = (
    cleaned
    .filter(F.year("shipped_date") >= 2025)
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "clean_carrier",
        F.max("cost").over(w_grp).alias("last3_max_cost"),
        F.sum("cost").over(w_grp).alias("last3_sum_cost"),
        F.avg("cost").over(w_grp).alias("last3_avg_cost"),
        F.min("cost").over(w_grp).alias("last3_min_cost"),
    )
)
