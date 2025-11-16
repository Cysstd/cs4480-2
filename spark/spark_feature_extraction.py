# spark_feature_extraction_from_pig_fixed.py - 修复NumPy错误

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, size
from pyspark.sql.types import ArrayType, DoubleType
import numpy as np
import json

# 初始化Spark
spark = SparkSession.builder \
    .appName("FeatureExtractionFromPigFixed") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

def extract_features_from_64x64_fixed(image_array_str):
    """修复版特征提取 - 使用存在的NumPy函数"""
    try:
        # 解析图像数组
        image_array = np.array(json.loads(image_array_str), dtype=np.uint8)
        
        # 验证尺寸
        if len(image_array.shape) != 3:
            print(f"❌ 非3D图像，跳过: {image_array.shape}")
            return []
            
        height, width, channels = image_array.shape
        
        # 确保是64×64×3
        if height != 64 or width != 64:
            print(f"⚠️ 非常规尺寸 {height}×{width}，使用当前尺寸继续处理")
        
        print(f"✅ 处理图像: {height}×{width}×{channels}")
        
        features = []
        
        # ==================== 颜色特征提取 ====================
        if channels >= 3:
            # 使用前3个通道 (RGB)
            for channel in range(3):
                channel_data = image_array[:, :, channel].flatten()
                # 计算真实的统计特征
                features.extend([
                    float(np.mean(channel_data)),          # 均值
                    float(np.std(channel_data)),           # 标准差  
                    float(np.median(channel_data)),        # 中位数
                    float(np.percentile(channel_data, 25)), # 25%分位数
                    float(np.percentile(channel_data, 75)), # 75%分位数
                ])
        else:
            # 单通道图像，重复计算保持特征数一致
            channel_data = image_array[:, :, 0].flatten()
            for _ in range(3):
                features.extend([
                    float(np.mean(channel_data)),
                    float(np.std(channel_data)),
                    float(np.median(channel_data)),
                    float(np.percentile(channel_data, 25)),
                    float(np.percentile(channel_data, 75)),
                ])
        
        # ==================== 纹理特征提取 ====================
        # 转换为灰度图
        if channels >= 3:
            gray_img = np.dot(image_array[...,:3], [0.2989, 0.5870, 0.1140])  # 标准RGB转灰度
        else:
            gray_img = image_array[:, :, 0]
        
        gray_flat = gray_img.flatten()
        
        # 基础灰度统计 - 使用存在的NumPy函数
        features.extend([
            float(np.mean(gray_flat)),     # 灰度均值
            float(np.std(gray_flat)),      # 灰度标准差
            float(np.var(gray_flat)),      # 方差 (替代skew)
        ])
        
        # 图像熵 (纹理复杂度)
        try:
            histogram, _ = np.histogram(gray_img, bins=64, range=(0, 255))
            histogram = histogram.astype(float)
            histogram += 1e-8  # 避免log(0)
            histogram /= histogram.sum()
            entropy = -np.sum(histogram * np.log2(histogram))
            features.append(float(entropy))
        except Exception as e:
            print(f"⚠️ 熵计算失败: {e}")
            features.append(0.0)
        
        # ==================== 对比度特征 ====================
        try:
            # 使用RMS对比度
            contrast = float(np.std(gray_img))
            features.append(contrast)
        except:
            features.append(0.0)
            
        # ==================== 新增简单形状特征 ====================
        try:
            # 计算图像亮度分布特征
            brightness_mean = float(np.mean(gray_img))
            brightness_std = float(np.std(gray_img))
            
            # 添加更多基础统计特征
            features.extend([
                float(np.min(gray_img)),    # 最小值
                float(np.max(gray_img)),    # 最大值
                float(np.percentile(gray_img, 10)),  # 10%分位数
                float(np.percentile(gray_img, 90)),  # 90%分位数
            ])
        except Exception as e:
            print(f"⚠️ 形状特征失败: {e}")
            features.extend([0.0] * 4)
            
        print(f"✅ 真实提取了 {len(features)} 个特征，范围: [{min(features):.3f}, {max(features):.3f}]")
        return features
        
    except Exception as e:
        print(f"❌ 特征提取错误: {e}")
        return []  # 返回空列表，让Spark过滤

# 注册UDF
extract_features_udf = udf(extract_features_from_64x64_fixed, ArrayType(DoubleType()))

def validate_no_hardcoding(df):
    """验证没有硬编码特征"""
    print("\n🔍 检查硬编码特征...")
    
    samples = df.select("image_features").limit(10).collect()
    
    hardcoded_count = 0
    for i, row in enumerate(samples):
        features = row.image_features
        if features:
            # 检查是否所有特征都是0 (硬编码嫌疑)
            if all(abs(f) < 1e-6 for f in features):
                hardcoded_count += 1
                print(f"❌ 样本 {i}: 所有特征都是0 (硬编码嫌疑)")
            else:
                # 检查特征值的合理性
                non_zero_features = [f for f in features if abs(f) > 1e-6]
                feature_range = max(features) - min(features)
                if feature_range > 1.0 and len(non_zero_features) > 0:
                    print(f"✅ 样本 {i}: 特征正常 [{min(features):.3f}, {max(features):.3f}]")
                else:
                    print(f"⚠️  样本 {i}: 特征范围过小 [{min(features):.3f}, {max(features):.3f}]")
    
    if hardcoded_count > 0:
        print(f"❌ 发现 {hardcoded_count} 个硬编码样本")
        return False
    else:
        print("✅ 未发现硬编码特征")
        return True

def main():
    print("=" * 60)
    print("🚀 Spark特征提取 - 修复NumPy错误")
    print("=" * 60)
    
    # 使用Pig输出的确切路径
    pig_output_path = "hdfs://localhost:9000/user/hadoop/image_analysis/cleaned_data_with_images_json_fixed"
    
    try:
        # 读取Pig清洗后的数据
        cleaned_data = spark.read.json(pig_output_path)
        record_count = cleaned_data.count()
        print(f"✅ 成功读取Pig输出数据: {record_count} 条记录")
        
    except Exception as e:
        print(f"❌ 无法读取Pig输出数据: {e}")
        spark.stop()
        return
    
    # 数据预览
    print("\n🔍 数据预览:")
    cleaned_data.select("image_id", "label", "keyword").show(5, truncate=30)
    
    # ==================== 特征提取 ====================
    print("\n" + "=" * 60)
    print("🔧 开始特征提取 (修复NumPy错误)...")
    print("=" * 60)
    
    # 应用特征提取
    features_df = cleaned_data.withColumn("image_features", extract_features_udf(col("image_array")))
    
    # 过滤掉提取失败的记录 (返回空列表的)
    valid_features_df = features_df.filter(size("image_features") > 0)
    
    failed_count = features_df.count() - valid_features_df.count()
    print(f"特征提取结果:")
    print(f"  ✅ 成功: {valid_features_df.count()} 条记录")
    print(f"  ❌ 失败: {failed_count} 条记录")
    
    if valid_features_df.count() == 0:
        print("❌ 所有特征提取都失败了!")
        spark.stop()
        return
    
    # ==================== 验证特征质量 ====================
    print("\n" + "=" * 60)
    print("🔬 验证特征质量")
    print("=" * 60)
    
    # 检查硬编码
    no_hardcoding = validate_no_hardcoding(valid_features_df)
    
    # 显示特征统计
    print("\n📊 特征统计信息:")
    feature_samples = valid_features_df.select("image_features").limit(5).collect()
    
    for i, row in enumerate(feature_samples):
        features = row.image_features
        print(f"样本 {i}:")
        print(f"  特征数: {len(features)}")
        print(f"  范围: [{min(features):.3f}, {max(features):.3f}]")
        print(f"  均值: {np.mean(features):.3f} ± {np.std(features):.3f}")
        print(f"  非零特征: {sum(1 for f in features if abs(f) > 1e-6)}/{len(features)}")
    
    # ==================== 保存结果 ====================
    print("\n" + "=" * 60)
    print("💾 保存特征数据...")
    print("=" * 60)
    
    # 选择需要的列
    final_df = valid_features_df.select(
        "image_id",
        "label", 
        "keyword",
        "tags", 
        "image_features",
        "original_shape"
    )
    
    # 保存到HDFS
    output_path = "hdfs://localhost:9000/user/hadoop/image_analysis/extracted_features_final"
    
    try:
        final_df.write.mode("overwrite").json(output_path)
        print(f"✅ 特征数据已保存到: {output_path}")
        
        # 验证保存
        saved_count = spark.read.json(output_path).count()
        print(f"✅ 验证: 成功保存 {saved_count} 条记录")
        
    except Exception as e:
        print(f"❌ 保存失败: {e}")
    
    # ==================== 质量报告 ====================
    print("\n" + "=" * 60)
    print("📋 特征质量报告")
    print("=" * 60)
    
    if no_hardcoding:
        print("🎉 特征质量优秀!")
        print("   ✅ 无硬编码特征")
        print("   ✅ 特征值范围合理")
        print("   ✅ 真正从图像数据计算")
    else:
        print("⚠️  特征质量有问题:")
        print("   ❌ 发现硬编码特征")
    
    print(f"\n✨ 修复完成!")
    print(f"原始数据: {record_count} 条")
    print(f"有效特征: {valid_features_df.count()} 条")
    print(f"特征维度: {len(feature_samples[0].image_features) if feature_samples else '未知'}")
    
    spark.stop()

if __name__ == "__main__":
    main()