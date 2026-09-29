from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Tokyo", "Berlin", "New York"]))
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)
