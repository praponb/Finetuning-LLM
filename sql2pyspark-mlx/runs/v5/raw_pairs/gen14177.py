from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.countDistinct("price").alias("unique_categorys"),
        F.min("cost").alias("min_cost"),
    )
    .orderBy("unique_categorys", "category")
    .limit(5)
)
