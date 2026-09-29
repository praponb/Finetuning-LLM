from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Initech"]) & (F.col("brand") == "Stark") & (F.col("supplier_id") == 16))
    .withColumn("category_group", F.when(F.col("category").isin(["Fashion", "Books"]), "Priority").otherwise("Other"))
)

category_summary = (
    base
    .groupBy("category")
    .agg(
        F.countDistinct("price").alias("unique_categorys"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("cost")).otherwise(0)).alias("acme_cost"),
    )
)

result = category_summary.orderBy(F.col("unique_categorys").desc())
