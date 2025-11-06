# config.py - 项目配置参数

# 网站列表
'''
WEBSITES_TO_SCRAPE = [
    'https://commons.wikimedia.org/wiki/Category:Cat_images',
    'https://commons.wikimedia.org/wiki/Category:Dog_images',
    'https://pixabay.com/images/search/cat/',
    'https://pixabay.com/images/search/dog/',
    'https://unsplash.com/s/photos/cat',
    'https://unsplash.com/s/photos/dog'
]
'''
WEBSITES_TO_SCRAPE = [
    'https://commons.wikimedia.org/w/index.php?search=cat&title=Special%3AMediaSearch&type=image'
]

# 图像处理配置
IMAGE_SIZE = (64, 64)  # 调整后的图像尺寸
MAX_IMAGES_PER_SITE = 2  # 每个网站最大图片数

# 特征提取配置
FEATURE_CONFIG = {
    'color_features': True,
    'texture_features': True, 
    'shape_features': True,
    'hog_features': True
}

# 机器学习配置
ML_CONFIG = {
    'test_size': 0.3,
    'random_state': 42,
    'n_estimators': 100
}

# 数据库配置
DB_CONFIG = {
    'mongodb_uri': "mongodb://localhost:27017/",
    'database_name': "image_analysis",
    'collection_names': ['image_metadata', 'text_analysis', 'analysis_results']
}