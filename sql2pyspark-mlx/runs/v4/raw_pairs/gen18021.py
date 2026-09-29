from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") < 45) & (F.col("segment") != "SMB") & (F.col("country") == "JP"))
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.min("signup_date").alias("first_signup_date"),
    )
    .orderBy("country_values", "country")
    .limit(50)
)
