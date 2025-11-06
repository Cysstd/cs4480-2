# parallel_scraper.py - 专注于src属性的版本
# 123123213
import time
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import multiprocessing as mp
from multiprocessing import Pool, cpu_count
import numpy as np

from utils import extract_image_urls_from_src, assign_label, download_image, resize_image, create_session
from config import WEBSITES_TO_SCRAPE, IMAGE_SIZE, MAX_IMAGES_PER_SITE

class ParallelWebScraper:
    def __init__(self):
        self.num_cores = cpu_count()
        self.all_images_data = []
        self.all_images = []
    
    def scrape_single_website(self, url):
        """爬取单个网站 - 专注于src属性"""
        website_images_data = []
        website_images = []
        local_session = create_session()
        
        try:
            print(f"🔍 Scraping: {url}")
            response = local_session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 使用专门从src属性提取URL的函数
            image_urls = extract_image_urls_from_src(soup, url)
            
            print(f"🖼️ Found {len(image_urls)} valid image URLs at {url}")
            
            # 下载和处理图片
            successful_downloads = 0
            for i, img_url in enumerate(image_urls[:MAX_IMAGES_PER_SITE]):
                try:
                    print(f"  [{i+1}/{min(len(image_urls), MAX_IMAGES_PER_SITE)}] Processing: {img_url[:70]}...")
                    
                    image = download_image(img_url, local_session)
                    if image is not None:
                        # 转换为RGB（处理PNG透明背景等问题）
                        if image.mode != 'RGB':
                            image = image.convert('RGB')
                            
                        image_array = np.array(image)
                        
                        # 检查图片尺寸和质量
                        if (len(image_array.shape) == 3 and  # 必须是彩色图片
                            image_array.shape[0] >= 50 and 
                            image_array.shape[1] >= 50 and
                            image_array.shape[2] == 3):  # 必须有3个颜色通道
                            
                            image_resized = resize_image(image, IMAGE_SIZE)
                            image_array_resized = np.array(image_resized)
                            
                            label = assign_label(img_url)
                            
                            image_data = {
                                'image_id': f"scraped_{mp.current_process().pid}_{i}",
                                'image_array': image_array_resized,
                                'source_url': img_url,
                                'description': f"Scraped from {urlparse(url).netloc}",
                                'download_timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                                'label': label,
                                'original_shape': image_array.shape
                            }
                            
                            website_images_data.append(image_data)
                            website_images.append(image_array_resized)
                            successful_downloads += 1
                            print(f"  ✅ Successfully stored image {successful_downloads}")
                        else:
                            print(f"  ⚠️  Image rejected: invalid shape {image_array.shape}")
                    else:
                        print(f"  ❌ Failed to process image")
                        
                except Exception as e:
                    print(f"  ❌ Error processing image {i+1}: {str(e)[:80]}")
                    continue
            
            print(f"✅ Downloaded {successful_downloads} images from {url}")
            
        except Exception as e:
            print(f"❌ Error scraping {url}: {e}")
        
        finally:
            local_session.close()
        
        return website_images_data, website_images
    
    def run_parallel_scraping(self, websites=None):
        """运行并行网页爬取"""
        if websites is None:
            websites = WEBSITES_TO_SCRAPE
        
        print("🚀 Starting parallel web scraping...")
        print(f"🖥️  CPU cores available: {self.num_cores}")
        print(f"🎯 Target websites: {len(websites)}")
        
        start_time = time.time()
        
        # 使用进程池进行并行爬取
        with Pool(processes=min(self.num_cores, len(websites))) as pool:
            results = pool.map(self.scrape_single_website, websites)
        
        # 合并结果
        for website_images_data, website_images in results:
            if website_images_data:
                self.all_images_data.extend(website_images_data)
                self.all_images.extend(website_images)
        
        scraping_time = time.time() - start_time
        print(f"⚡ Parallel scraping completed in {scraping_time:.2f} seconds")
        print(f"📊 Total images scraped: {len(self.all_images_data)}")
        
        return self.all_images_data, self.all_images, scraping_time

    def create_test_data(self):
        """创建测试数据（如果爬取失败）"""
        print("🛠️ Creating test data for development...")
        
        # 创建一些简单的测试图片
        test_images_data = []
        test_images = []
        
        for i in range(12):  # 创建12张测试图片
            # 创建随机图片
            if i % 2 == 0:
                # 创建"猫"图片（偏橙色）
                img_array = np.random.randint(200, 255, (64, 64, 3), dtype=np.uint8)
                img_array[:, :, 0] = np.random.randint(200, 255, (64, 64))  # 更多红色
                img_array[:, :, 2] = np.random.randint(0, 100, (64, 64))    # 较少蓝色
                label = 0
                desc = "Test cat image from src scraping"
            else:
                # 创建"狗"图片（偏棕色）
                img_array = np.random.randint(150, 200, (64, 64, 3), dtype=np.uint8)
                img_array[:, :, 0] = np.random.randint(150, 200, (64, 64))  # 中等红色
                img_array[:, :, 1] = np.random.randint(100, 150, (64, 64))  # 中等绿色
                label = 1
                desc = "Test dog image from src scraping"
            
            image_data = {
                'image_id': f"test_{i}",
                'image_array': img_array,
                'source_url': f"https://example.com/test_{i}.jpg",
                'description': desc,
                'download_timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                'label': label,
                'original_shape': img_array.shape
            }
            
            test_images_data.append(image_data)
            test_images.append(img_array)
        
        print(f"📊 Created {len(test_images_data)} test images")
        return test_images_data, test_images, 0.1

if __name__ == "__main__":
    # 单独测试爬取模块
    scraper = ParallelWebScraper()
    
    try:
        images_data, images, time_taken = scraper.run_parallel_scraping()
        if len(images_data) == 0:
            print("⚠️  No images scraped from src attributes, creating test data...")
            images_data, images, time_taken = scraper.create_test_data()
    except Exception as e:
        print(f"❌ Scraping failed: {e}")
        print("🛠️ Creating test data instead...")
        images_data, images, time_taken = scraper.create_test_data()
    
    print(f"测试完成: 处理 {len(images_data)} 张图片, 耗时 {time_taken:.2f}秒")