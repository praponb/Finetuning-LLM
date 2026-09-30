from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.sum("revenue").alias("total_revenue"))
    .withColumn("pct_of_total", F.round(((F.col("total_revenue") * 100) / F.sum("total_revenue").over(w)), 2))
)
