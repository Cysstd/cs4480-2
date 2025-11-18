# config.py - 项目配置参数

# Pixabay API 配置
PIXABAY_CONFIG = {
    'api_key': '53219156-524e893e76922fa46c413a669',
    'base_url': 'https://pixabay.com/api/',
    'headers': {
        'User-Agent': 'ImageAnalysisBot/1.0'
    }
}

# 搜索关键词
SEARCH_KEYWORDS = ['cat', 'dog']

# 图像处理配置
IMAGE_SIZE = (64, 64)
MAX_IMAGES_PER_KEYWORD = 20

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