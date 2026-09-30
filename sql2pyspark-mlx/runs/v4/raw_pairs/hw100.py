from pyspark.sql import functions as F

result = df_web_events.groupBy("session_id").agg(F.sort_array(F.collect_list("page")).alias("pages"))
