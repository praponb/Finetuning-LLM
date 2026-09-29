from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2023)
    .groupBy("carrier")
    .agg(F.sort_array(F.collect_set("carrier")).alias("carrier_list"))
)
