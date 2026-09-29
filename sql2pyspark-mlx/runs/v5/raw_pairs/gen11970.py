from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "Berlin", "London"]))
    .groupBy(
        F.when(F.col("country").isin(["JP", "SG"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
    .orderBy(F.col("country_group").desc())
    .limit(3)
)
