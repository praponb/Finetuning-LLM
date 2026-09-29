from pyspark.sql import functions as F

result = (
    df_products
    .withColumn("launch_date_label", F.date_format("launch_date", "yyyy-MM"))
    .withColumn("category_upper", F.upper("category"))
    .orderBy(F.col("id").desc(), "launch_date_label")
)
