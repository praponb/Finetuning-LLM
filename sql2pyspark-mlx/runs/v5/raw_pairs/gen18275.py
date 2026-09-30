from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
    )
    .orderBy("country")
    .limit(50)
)
