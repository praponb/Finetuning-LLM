from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(~F.col("country").isin(["JP", "UK", "US"]))

agg = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

result = agg.orderBy("city")
