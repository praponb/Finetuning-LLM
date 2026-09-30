from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_cohort")

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([1, 4, 9]))
    .groupBy(
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
    .orderBy("cumulative_cost")
)
