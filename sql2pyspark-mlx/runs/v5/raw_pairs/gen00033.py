from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("status").orderBy("cost", "id")

cleaned = (
    df_shipments
    .filter(F.col("delivered_date") > F.lit("2023-03-18").cast("date"))
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("status").isin(["RETURNED", "LOST"]), "Priority").otherwise("Other").alias("status_group"),
    )
)

result = (
    cleaned
    .filter(F.col("cost").isNotNull())
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        F.avg("cost").over(w).alias("cost_avg_5_prior"),
        F.ntile(3).over(w2).alias("cost_bucket"),
        F.lag("cost", 1).over(w3).alias("prev_cost"),
    )
)
