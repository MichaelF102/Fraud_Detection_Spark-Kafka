import os
import sys
import urllib.request
import pyspark

def main():
    jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
    os.makedirs(jars_dir, exist_ok=True)
    
    jars = [
        ("https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.jar", "spark-sql-kafka-0-10_2.12-3.5.8.jar"),
        ("https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.8/spark-token-provider-kafka-0-10_2.12-3.5.8.jar", "spark-token-provider-kafka-0-10_2.12-3.5.8.jar"),
        ("https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar", "kafka-clients-3.5.1.jar"),
        ("https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar", "commons-pool2-2.12.0.jar"),
        ("https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar", "postgresql-42.7.5.jar"),
    ]
    
    print(f"Downloading Spark streaming dependencies to {jars_dir}...")
    for url, filename in jars:
        target_path = os.path.join(jars_dir, filename)
        if not os.path.exists(target_path):
            print(f"Fetching {filename}...")
            urllib.request.urlretrieve(url, target_path)
            print(f"Downloaded {filename} ({os.path.getsize(target_path)} bytes).")
        else:
            print(f"Found existing {filename}.")
            
    print("All Spark streaming jars successfully configured!")

if __name__ == "__main__":
    main()
