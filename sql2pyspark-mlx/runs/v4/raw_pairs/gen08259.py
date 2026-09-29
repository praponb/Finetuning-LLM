from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("city") != "Singapore")

agg = (
    src
    .groupBy("city")
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

result = (
    agg
    .orderBy("city")
    .limit(20)
)
