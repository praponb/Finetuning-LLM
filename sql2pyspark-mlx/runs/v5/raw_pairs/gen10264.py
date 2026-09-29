from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier")

metrics = (
    df_shipments
    .filter(F.col("cost") > 56)
    .withColumn("running_cost", F.sum("cost").over(w_part))
    .withColumn("running_cost_2", F.sum("cost").over(w2))
    .withColumn("running_cost_3", F.sum("cost").over(w3))
    .withColumn("carrier_count_cost", F.count("cost").over(w4))
)

result = (
    metrics
    .select(
        "id",
        "shipped_date",
        "cost",
        "running_cost",
        "running_cost_2",
        "running_cost_3",
        "carrier_count_cost",
    )
    .orderBy("running_cost", F.col("cost").desc())
)
