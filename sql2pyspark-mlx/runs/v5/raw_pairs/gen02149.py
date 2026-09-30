from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.sum("rating").alias("total_rating"),
    )
    .orderBy(F.col("country").desc())
)
