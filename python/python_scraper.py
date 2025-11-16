# python_scraper.py - 修复版本

import time
import os
from urllib.parse import urlparse
import numpy as np
import json
import subprocess
import sys

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import (
    download_image, resize_image, create_session, 
    validate_image_content, search_wikimedia_images, assign_label
)
from config import SEARCH_KEYWORDS, IMAGE_SIZE, MAX_IMAGES_PER_KEYWORD, PIXABAY_CONFIG

class PythonImageScraper:
    def __init__(self, save_original_images=True, save_dir=None):
        self.all_images_data = []
        self.all_images = []
        self.save_original_images = save_original_images
        
        if save_dir is None:
            self.save_dir = os.path.join(os.getcwd(), "pixabay_images")
        else:
            self.save_dir = save_dir
        
        if self.save_original_images:
            try:
                os.makedirs(self.save_dir, exist_ok=True)
                print(f"📁 图片将保存到: {self.save_dir}")
            except Exception as e:
                print(f"❌ 无法创建目录: {e}")
                self.save_original_images = False
    
    def scrape_and_save_to_hdfs(self, keywords=None):
        if keywords is None:
            keywords = SEARCH_KEYWORDS
        
        print("🚀 Python爬虫开始工作...")
        start_time = time.time()
        
        for keyword in keywords:
            self._scrape_keyword(keyword)
        
        # 如果没有获取到任何数据，直接失败
        if len(self.all_images_data) == 0:
            print("❌ 错误：没有获取到任何图片数据！")
            return None, 0
        
        metadata_file = self._save_metadata()
        self._upload_to_hdfs(metadata_file)
        
        scraping_time = time.time() - start_time
        print(f"✅ Python爬虫完成，耗时: {scraping_time:.2f}秒")
        
        return self.all_images_data, scraping_time
    
    def _scrape_keyword(self, keyword):
        print(f"🎯 爬取关键词: {keyword}")
        
        local_session = create_session()
        
        # 获取图片数据（包含标签信息）
        images_data = search_wikimedia_images(keyword, MAX_IMAGES_PER_KEYWORD)
        
        if not images_data:
            print(f"❌ 无法获取 {keyword} 图片数据")
            return
        
        successful_downloads = 0
        for i, img_data in enumerate(images_data):
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
                    
                    if validate_image_content(image_array_resized):
                        # 使用基于tags的标签，如果不可用则使用旧的assign_label
                        label = img_data.get('label')
                        if label == -1:  # 如果无法基于tags确定标签
                            label = assign_label(img_url, keyword)
                        
                        image_data = {
                            'image_id': f"{keyword}_{img_data.get('api_id', i)}",
                            'image_array': image_array_resized.tolist(),
                            'source_url': img_url,
                            'description': f"Pixabay {keyword} image",
                            'download_timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                            'label': label,
                            'keyword': keyword,
                            'original_shape': list(np.array(image).shape),
                            'tags': img_data.get('tags', ''),
                            'views': img_data.get('views', 0),
                            'downloads': img_data.get('downloads', 0),
                            'user': img_data.get('user', '')
                        }
                        
                        self.all_images_data.append(image_data)
                        self.all_images.append(image_array_resized)
                        successful_downloads += 1
                        print(f"    ✅ {keyword} 图片 {successful_downloads} 爬取成功 (标签: {label})")
                        
            except Exception as e:
                print(f"    ❌ 处理失败: {e}")
                continue
        
        if successful_downloads == 0:
            print(f"❌ {keyword} 没有成功下载任何图片")
        else:
            print(f"✅ {keyword} 成功下载 {successful_downloads} 张图片")
    
    def _save_metadata(self):
        metadata_file = "image_metadata.json"
        try:
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.all_images_data, f, indent=2, ensure_ascii=False)
            print(f"💾 元数据保存到: {metadata_file}")
            return metadata_file
        except Exception as e:
            print(f"❌ 保存元数据失败: {e}")
            return None
    
    def _upload_to_hdfs(self, metadata_file):
        try:
            # 创建HDFS目录
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/user/hadoop/image_analysis"], 
                         check=True, capture_output=True)
            
            if metadata_file and os.path.exists(metadata_file):
                subprocess.run(["hdfs", "dfs", "-put", "-f", metadata_file, 
                              "/user/hadoop/image_analysis/"], 
                             check=True, capture_output=True)
                print("✅ 元数据上传到HDFS: /user/hadoop/image_analysis/image_metadata.json")
            
            if self.save_original_images and os.path.exists(self.save_dir):
                subprocess.run(["hdfs", "dfs", "-put", "-f", self.save_dir, 
                              "/user/hadoop/image_analysis/"], 
                             check=True, capture_output=True)
                print("✅ 图片数据上传到HDFS")
                
        except subprocess.CalledProcessError as e:
            print(f"❌ HDFS上传失败: {e}")
            print("💡 请确保Hadoop服务已启动: start-dfs.sh && start-yarn.sh")
        except Exception as e:
            print(f"❌ HDFS操作错误: {e}")

if __name__ == "__main__":
    scraper = PythonImageScraper(save_original_images=True)
    images_data, time_taken = scraper.scrape_and_save_to_hdfs()
    
    if images_data:
        print(f"🎯 总共获取 {len(images_data)} 张图片")
        
        # 简单统计
        cat_count = sum(1 for img in images_data if img['label'] == 0)
        dog_count = sum(1 for img in images_data if img['label'] == 1)
        print(f"📊 分类统计: 猫({cat_count}), 狗({dog_count})")
    else:
        print("❌ 爬虫失败：没有获取到任何数据")