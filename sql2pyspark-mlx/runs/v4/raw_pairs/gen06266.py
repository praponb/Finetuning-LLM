from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_cohort")

result = (
    df_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.sum("revenue").alias("total_revenue"))
    .withColumn("cumulative_revenue", F.sum("total_revenue").over(w))
    .orderBy(F.col("total_revenue").desc())
)
