from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~((F.col("city") == "Tokyo") & F.col("city").isin(["Bangkok", "Berlin", "Tokyo"])))
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.max("rating").alias("max_rating"),
    )
)
