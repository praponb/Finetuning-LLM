from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.year("launch_date") == 2023) & F.col("cost").between(250, 560))
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
)
