from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Kerry", "FedEx"]))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.count("cost").over(w).alias("running_count_cost"),
        F.avg("cost").over(w).alias("running_avg_cost"),
    )
)
