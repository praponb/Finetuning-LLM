from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost")

ranked = (
    df_products
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        F.when(F.col("category").rlike("^(Fashion|Home)$"), "GROUP_B").otherwise("CORE").alias("category_zone"),
    )
    .orderBy(F.col("brand").asc_nulls_first(), F.col("category").desc())
)
