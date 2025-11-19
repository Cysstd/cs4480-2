# My_Project/python/spark_scraper_cluster.py (COMPLETE AND CORRECTED)

import time
import json
import subprocess
from pyspark.sql import SparkSession
from config import SEARCH_KEYWORDS, IMAGE_SIZE, MAX_IMAGES_PER_KEYWORD

def save_and_upload(all_data, local_filename="image_metadata.json"):
    """Saves data to a local JSON file and uploads it to HDFS."""
    print("\n💾 Saving metadata locally...")
    with open(local_filename, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)
    print(f"   Saved to {local_filename}")

    print("🌍 Uploading to HDFS...")
    try:
        # Use subprocess to run HDFS commands
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/user/hadoop/image_analysis"], check=True, capture_output=True)
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_filename, "/user/hadoop/image_analysis/image_metadata.json"], check=True, capture_output=True)
        print("   ✅ Successfully uploaded to HDFS.")
    except subprocess.CalledProcessError as e:
        print(f"   ❌ HDFS upload failed: {e.stderr.decode()}")
    except Exception as e:
        print(f"   ❌ An error occurred during HDFS upload: {e}")

if __name__ == "__main__":

    def scrape_keyword_worker_spark(keyword):
        """
        This is the full worker function that will be sent to Spark workers.
        It must contain all its own imports.
        """
        import time
        import numpy as np
        # These imports will work because of the --py-files argument in spark-submit
        from utils import download_image, resize_image, create_session, search_wikimedia_images, assign_label, validate_image_content
        
        print(f"🚀 [Spark Worker] Starting task for keyword: {keyword}")
        local_session = create_session()
        images_data_for_keyword = []
        
        # Use MAX_IMAGES_PER_KEYWORD from config
        api_results = search_wikimedia_images(keyword, MAX_IMAGES_PER_KEYWORD)
        
        if not api_results:
            print(f"❌ [Spark Worker] No API results for {keyword}")
            return []

        successful_downloads = 0
        for i, img_data in enumerate(api_results):
            if successful_downloads >= MAX_IMAGES_PER_KEYWORD:
                break
            try:
                img_url = img_data.get('image_url')
                if not img_url:
                    continue
                
                image = download_image(img_url, local_session)
                if image and validate_image_content(np.array(image)):
                    image_resized = resize_image(image, IMAGE_SIZE)
                    image_array_resized = np.array(image_resized)
                    
                    label = img_data.get('label', -1)
                    if label == -1:
                        label = assign_label(img_url, keyword)
                    
                    image_data = {
                        'image_id': f"{keyword}_{img_data.get('api_id', i)}",
                        'image_array': image_array_resized.tolist(),
                        'source_url': img_url,
                        'label': label,
                        'keyword': keyword,
                        'tags': img_data.get('tags', ''),
                        'original_shape': list(np.array(image).shape),
                        'views': img_data.get('views', 0),
                        'downloads': img_data.get('downloads', 0),
                        'user': img_data.get('user', '')
                    }
                    images_data_for_keyword.append(image_data)
                    successful_downloads += 1
            except Exception as e:
                # In a distributed environment, it's better to just print the error and continue
                print(f"    ❌ [Spark Worker] Error processing image for '{keyword}': {e}")
        
        print(f"✅ [Spark Worker] Task for '{keyword}' finished, found {len(images_data_for_keyword)} images.")
        return images_data_for_keyword

    print("--- Experiment 3: Distributed Scraping with Spark Cluster ---")
    start_time = time.time()

    spark = SparkSession.builder.appName("DistributedScraper").getOrCreate()
        
    print(f"🔥 Distributing scraping tasks for keywords: {SEARCH_KEYWORDS}")
    keyword_rdd = spark.sparkContext.parallelize(SEARCH_KEYWORDS)
    image_data_rdd = keyword_rdd.flatMap(scrape_keyword_worker_spark)
    
    print("📊 Collecting results from Spark workers...")
    all_images_data = image_data_rdd.collect()
    
    spark.stop()
    end_time = time.time()

    if all_images_data:
        save_and_upload(all_images_data)
    else:
        print("❌ No data was scraped.")

    print("\n-----------------------------------------")
    print(f"✅ Spark cluster scraping finished in {end_time - start_time:.2f} seconds.")
    print(f"📊 Total images found: {len(all_images_data)}")
    print("-----------------------------------------")