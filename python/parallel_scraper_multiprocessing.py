# My_Project/python/parallel_scraper_multiprocessing.py
# Description: Uses Python's multiprocessing to scrape keywords in parallel on a single machine.

import time
import json
import subprocess
from multiprocessing import Pool, cpu_count
from utils import download_image, resize_image, create_session, search_wikimedia_images, assign_label, validate_image_content
from config import SEARCH_KEYWORDS, IMAGE_SIZE, MAX_IMAGES_PER_KEYWORD
import numpy as np

# (The scrape_keyword_worker and save_and_upload functions are identical to the sequential_scraper.py)
# ... Just copy them here ...
def scrape_keyword_worker(keyword):
    """Worker function to scrape all images for a given keyword."""
    print(f"🚀 Starting task for keyword: {keyword}")
    # ... (This function is identical to the one in the previous answer) ...
    # ... It downloads images and returns a list of dictionaries ...
    local_session = create_session()
    images_data_for_keyword = []
    api_results = search_wikimedia_images(keyword, MAX_IMAGES_PER_KEYWORD)
    if not api_results:
        print(f"❌ No API results for {keyword}")
        return []

    successful_downloads = 0
    for i, img_data in enumerate(api_results):
        if successful_downloads >= MAX_IMAGES_PER_KEYWORD: break
        try:
            img_url = img_data.get('image_url')
            if not img_url: continue
            image = download_image(img_url, local_session)
            if image and validate_image_content(np.array(image)):
                image_resized = resize_image(image, IMAGE_SIZE)
                image_array_resized = np.array(image_resized)
                label = img_data.get('label', -1)
                if label == -1: label = assign_label(img_url, keyword)
                
                image_data = {
                    'image_id': f"{keyword}_{img_data.get('api_id', i)}",
                    'image_array': image_array_resized.tolist(), 'source_url': img_url,
                    'label': label, 'keyword': keyword, 'tags': img_data.get('tags', ''),
                    'original_shape': list(np.array(image).shape),
                    'views': img_data.get('views', 0), 'downloads': img_data.get('downloads', 0),
                    'user': img_data.get('user', '')
                }
                images_data_for_keyword.append(image_data)
                successful_downloads += 1
                print(f"    ✅ [{keyword}] Image {successful_downloads}/{MAX_IMAGES_PER_KEYWORD} scraped.")
        except Exception as e:
            print(f"    ❌ [{keyword}] Processing failed: {e}")
            
    print(f"✅ Task for '{keyword}' finished, found {len(images_data_for_keyword)} images.")
    return images_data_for_keyword

def save_and_upload(all_data, local_filename="image_metadata.json"):
    """Saves data to a local JSON file and uploads it to HDFS."""
    print("\n💾 Saving metadata locally...")
    with open(local_filename, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)
    print(f"   Saved to {local_filename}")

    print("🌍 Uploading to HDFS...")
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/user/hadoop/image_analysis"], check=True)
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_filename, "/user/hadoop/image_analysis/image_metadata.json"], check=True)
        print("   ✅ Successfully uploaded to HDFS.")
    except Exception as e:
        print(f"   ❌ HDFS upload failed: {e}")


if __name__ == "__main__":
    print("--- Experiment 2: Single-Machine Parallel Scraping (multiprocessing) ---")
    start_time = time.time()
    
    num_processes = min(len(SEARCH_KEYWORDS), cpu_count())
    print(f"🔩 Using {num_processes} parallel processes.")

    with Pool(processes=num_processes) as pool:
        list_of_results = pool.map(scrape_keyword_worker, SEARCH_KEYWORDS)

    all_images_data = [item for sublist in list_of_results for item in sublist]
    
    end_time = time.time()

    if all_images_data:
        save_and_upload(all_images_data)
    else:
        print("❌ No data was scraped.")

    print("\n-----------------------------------------")
    print(f"✅ Multiprocessing scraping finished in {end_time - start_time:.2f} seconds.")
    print(f"📊 Total images found: {len(all_images_data)}")
    print("-----------------------------------------")