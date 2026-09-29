from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter((F.col("rating") < 2.5) & F.col("rating").between(3.5, 4.1) & F.col("country").isin(["UK", "JP", "SG"]))
)

summary = (
    filtered_suppliers
    .groupBy("city")
    .agg(F.avg("rating").alias("avg_rating"))
)

result = summary.filter(F.col("avg_rating") >= 2.5)
