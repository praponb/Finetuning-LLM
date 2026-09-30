from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("country").isin(["TH", "SG"]), "group_a").otherwise("other").alias("country_group"),
    )
    .agg(F.count("rating").alias("rating_count"))
    .orderBy("country_group")
)
