# utils.py - 修复 get_pixabay_images 函数

import re
import random
import time
from urllib.parse import urlparse
import requests
from PIL import Image
import numpy as np
from io import BytesIO
from datetime import datetime

from config import PIXABAY_CONFIG, IMAGE_SIZE, MAX_IMAGES_PER_KEYWORD

def get_pixabay_images(search_keyword, max_images=10):
    """使用Pixabay API获取图片和完整数据"""
    print(f"🔍 Pixabay搜索: {search_keyword}")
    
    images_data = []
    
    try:
        params = {
            'key': PIXABAY_CONFIG['api_key'],
            'q': search_keyword,
            'image_type': 'photo',
            'per_page': max_images,
            'safesearch': 'true',
            'order': 'popular'
        }
        
        session = create_session()
        response = session.get(
            PIXABAY_CONFIG['base_url'],
            params=params,
            timeout=15
        )
        response.raise_for_status()
        
        data = response.json()
        
        if 'hits' in data:
            images = data['hits']
            print(f"📊 Pixabay返回 {len(images)} 个图片")
            
            for img in images:
                try:
                    # 获取完整API数据
                    image_info = {
                        'api_id': img.get('id'),
                        'page_url': img.get('pageURL'),
                        'image_url': img.get('webformatURL'),
                        'tags': img.get('tags', ''),
                        'views': img.get('views', 0),
                        'downloads': img.get('downloads', 0),
                        'user': img.get('user', ''),
                    }
                    
                    # 基于tags进行简单分类
                    label = infer_label_from_tags(image_info['tags'])
                    
                    images_data.append({
                        **image_info,
                        'label': label
                    })
                    print(f"✅ 获取图片数据: {image_info['tags'][:30]}...")
                    
                except Exception as e:
                    print(f"❌ 处理图片数据失败: {e}")
                    continue
                    
        else:
            print("❌ Pixabay API返回数据格式不正确")
            
    except Exception as e:
        print(f"❌ Pixabay API请求失败: {e}")
    
    return images_data

def infer_label_from_tags(tags):
    """基于tags进行简单分类"""
    if not tags:
        return -1
    
    tags_lower = tags.lower()
    
    # 简单关键词匹配
    cat_count = sum(1 for keyword in ['cat', 'kitten', 'feline'] if keyword in tags_lower)
    dog_count = sum(1 for keyword in ['dog', 'puppy', 'canine'] if keyword in tags_lower)
    
    if cat_count > dog_count:
        return 0  # 猫
    elif dog_count > cat_count:
        return 1  # 狗
    else:
        return -1  # 无法确定

# 其余函数保持不变...
def search_wikimedia_images(search_keyword, max_images=10):
    return get_pixabay_images(search_keyword, max_images)

def is_valid_image_url(url):
    if not url or url.strip() == '':
        return False
    
    valid_extensions = ['.jpg', '.jpeg', '.png','.webp']
    parsed_url = urlparse(url)
    path = parsed_url.path.lower()
    
    if any(path.endswith(ext) for ext in valid_extensions):
        return True
    
    if 'pixabay.com' in parsed_url.netloc or 'cdn.pixabay.com' in parsed_url.netloc:
        return True
    
    return False

def download_and_validate_image(img_url, session, timeout=15):
    try:
        print(f"⬇️  下载图片...")
        
        img_response = session.get(img_url, timeout=timeout)
        if img_response.status_code == 200:
            content_type = img_response.headers.get('content-type', '').lower()
            if not content_type.startswith('image/'):
                return None
            
            content_length = img_response.headers.get('content-length')
            if content_length and int(content_length) < 10000:
                return None
            
            try:
                image = Image.open(BytesIO(img_response.content))
                
                if image.format not in ['JPEG', 'PNG', 'GIF', 'WEBP']:
                    return None
                
                if image.mode not in ['L', 'RGB', 'RGBA']:
                    return None
                
                if image.size[0] < 100 or image.size[1] < 100:
                    return None
                
                print(f"✅ 图片下载成功: {image.format}格式, {image.mode}模式, {image.size}尺寸")
                return image
                
            except Exception as e:
                print(f"❌ 无法解析图片: {e}")
                return None
        else:
            print(f"❌ HTTP {img_response.status_code}")
    except Exception as e:
        print(f"❌ 图片下载失败: {e}")
    
    return None

def assign_label(url, keyword=None):
    if keyword:
        keyword_lower = keyword.lower()
        if 'cat' in keyword_lower:
            return 0
        elif 'dog' in keyword_lower:
            return 1
    
    url_lower = url.lower()
    if 'cat' in url_lower:
        return 0
    elif 'dog' in url_lower:
        return 1
    
    return random.randint(0, 1)

def download_image(img_url, session, timeout=15):
    return download_and_validate_image(img_url, session, timeout)

def resize_image(image, size=(64, 64)):
    try:
        if image.mode != 'RGB':
            image = image.convert('RGB')
        return image.resize(size)
    except Exception as e:
        print(f"❌ 图片调整大小失败: {e}")
        return image

def create_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'ImageAnalysisBot/1.0',
        'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    })
    return session

def validate_image_content(image_array):
    if image_array is None:
        return False
    
    if len(image_array.shape) < 2:
        return False
    
    if image_array.shape[0] < 10 or image_array.shape[1] < 10:
        return False
    
    if len(image_array.shape) == 3:
        unique_colors = len(np.unique(image_array.reshape(-1, image_array.shape[2]), axis=0))
        if unique_colors < 10:
            return False
    
    return True