from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("carrier")).alias("carrier_list"))
    .orderBy("status")
)
