# utils.py - 专注于src属性的改进版本

import re
import random
from urllib.parse import urlparse, urljoin
import requests
from PIL import Image
import numpy as np
from io import BytesIO
from datetime import datetime

def is_valid_image_url(url):
    """改进的图片URL检测 - 专注于src属性"""
    if not url or url.strip() == '':
        return False
    
    # 首先检查常见的图片文件扩展名
    valid_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
    parsed_url = urlparse(url)
    path_lower = parsed_url.path.lower()
    
    # 检查文件扩展名
    if any(path_lower.endswith(ext) for ext in valid_extensions):
        return True
    
    # 检查常见图片域名和路径
    image_domains = ['wikimedia', 'upload.wikimedia', 'staticflickr', 'images.pexels']
    image_paths = ['/wiki/Special:FilePath/', '/thumb/', '/static/', '/images/']
    
    domain_valid = any(domain in parsed_url.netloc.lower() for domain in image_domains)
    path_valid = any(path in parsed_url.path for path in image_paths)
    
    return domain_valid or path_valid

def extract_image_urls_from_src(soup, base_url):
    """专门从src属性提取图片URL"""
    img_urls = []
    
    print(f"🔍 Searching for img tags with src attributes...")
    
    # 查找所有img标签的src属性
    img_tags = soup.find_all('img')
    print(f"📷 Found {len(img_tags)} img tags total")
    
    for i, img in enumerate(img_tags):
        src = img.get('src')
        if src:
            # 构建完整URL
            full_url = urljoin(base_url, src)
            
            if is_valid_image_url(full_url):
                img_urls.append(full_url)
                print(f"  ✅ Valid image URL: {full_url[:80]}...")
            else:
                print(f"  ❌ Invalid image URL (skipped): {full_url[:80]}...")
        else:
            # 检查其他可能的属性，但主要关注src
            for attr in ['data-src', 'data-lazy-src']:
                alt_src = img.get(attr)
                if alt_src:
                    full_url = urljoin(base_url, alt_src)
                    if is_valid_image_url(full_url):
                        img_urls.append(full_url)
                        print(f"  ✅ Valid image URL from {attr}: {full_url[:80]}...")
    
    # 去重
    unique_urls = list(set(img_urls))
    print(f"📊 Unique valid image URLs found: {len(unique_urls)}")
    
    return unique_urls

def assign_label(url):
    """基于URL模式分配标签"""
    url_lower = url.lower()
    
    cat_patterns = ['cat', 'kitten', 'feline', 'kitty']
    dog_patterns = ['dog', 'puppy', 'canine', 'pup']
    
    for pattern in cat_patterns:
        if pattern in url_lower:
            return 0  # 猫
    
    for pattern in dog_patterns:
        if pattern in url_lower:
            return 1  # 狗
    
    # 如果没有明确模式，随机分配
    return random.randint(0, 1)

def clean_text_description(description, stop_words=None):
    """清洗文本描述"""
    if description is None:
        return ""
        
    if stop_words is None:
        stop_words = set(['the', 'a', 'an', 'in', 'on', 'at', 'and', 'or', 'but'])
    
    clean_desc = re.sub(r'[^\w\s]', '', str(description).lower())
    words = clean_desc.split()
    filtered_words = [word for word in words if word not in stop_words and len(word) > 2]
    return ' '.join(filtered_words)

def download_image(img_url, session, timeout=8):
    """下载单张图片 - 专注于src URL"""
    try:
        print(f"    📥 Attempting download: {img_url[:60]}...")
        
        # 确保URL是完整的
        if not img_url.startswith(('http://', 'https://')):
            print(f"    ❌ Invalid URL scheme: {img_url}")
            return None
            
        img_response = session.get(img_url, timeout=timeout, stream=True)
        
        if img_response.status_code == 200:
            # 检查内容类型
            content_type = img_response.headers.get('content-type', '')
            if 'image' not in content_type:
                print(f"    ❌ Not an image (content-type: {content_type})")
                return None
                
            # 尝试打开图片
            try:
                image = Image.open(BytesIO(img_response.content))
                
                # 检查图片是否有效
                if image.size[0] > 10 and image.size[1] > 10:
                    print(f"    ✅ Successfully downloaded image: {image.size}")
                    return image
                else:
                    print(f"    ❌ Image too small: {image.size}")
                    return None
                    
            except Exception as img_error:
                print(f"    ❌ Cannot open as image: {img_error}")
                return None
        else:
            print(f"    ❌ HTTP {img_response.status_code}")
            return None
            
    except Exception as e:
        print(f"    ❌ Download failed: {str(e)[:50]}")
        return None

def resize_image(image, size=(64, 64)):
    """调整图片大小"""
    try:
        return image.resize(size, Image.Resampling.LANCZOS)
    except Exception as e:
        print(f"❌ Image resize failed: {e}")
        return image

def create_session():
    """创建请求会话"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    })
    return session