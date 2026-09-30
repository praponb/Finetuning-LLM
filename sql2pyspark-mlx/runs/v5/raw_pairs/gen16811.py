from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2023-07-01", "2023-10-19"))
    .groupBy("category")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("cost_count") < 7)
    .orderBy("category")
    .limit(50)
)
