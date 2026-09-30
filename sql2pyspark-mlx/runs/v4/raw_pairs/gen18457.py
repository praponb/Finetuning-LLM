from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")

deduped = (
    df_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .withColumn("age_tier", F.when(F.col("age") >= 65, "PREMIUM").otherwise("STANDARD"))
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 17600, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["age_tier", "credit_limit_tier"], ["age_tier"], []],
        "age_tier",
        "credit_limit_tier",
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("age_per_berlin"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("credit_limit_per_chiang_mai"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
)
