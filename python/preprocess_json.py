# preprocess_json_fixed.py - JSON格式预处理

import json
import os
import subprocess

def preprocess_json_for_pig():
    """为Pig处理准备JSON数据"""
    print("🔄 开始准备Pig处理数据...")
    
    input_path = "/user/hadoop/image_analysis/image_metadata.json"
    output_path = "/user/hadoop/image_analysis/image_metadata_line_by_line.json"
    
    try:
        # 从HDFS下载原始JSON
        print("📥 从HDFS下载原始JSON...")
        download_result = subprocess.run(
            ["hdfs", "dfs", "-cat", input_path], 
            capture_output=True, text=True, check=True
        )
        
        # 解析JSON数据
        print("🔍 解析JSON数据...")
        data = json.loads(download_result.stdout)
        
        print(f"📊 找到 {len(data)} 条记录")
        
        # 转换为每行一个JSON对象
        print("📝 转换为每行一个JSON对象...")
        with open('temp_line_by_line.json', 'w', encoding='utf-8') as f:
            for item in data:
                json_line = json.dumps(item, ensure_ascii=False)
                f.write(json_line + '\n')
        
        # 上传到HDFS
        print("⬆️ 上传到HDFS...")
        subprocess.run([
            "hdfs", "dfs", "-put", "-f", "temp_line_by_line.json", output_path
        ], check=True)
        
        # 清理临时文件
        if os.path.exists('temp_line_by_line.json'):
            os.remove('temp_line_by_line.json')
        
        print(f"✅ 数据准备完成！")
        print(f"📁 输出文件: {output_path}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ HDFS操作失败: {e}")
        return False
    except json.JSONDecodeError as e:
        print(f"❌ JSON解析失败: {e}")
        return False
    except Exception as e:
        print(f"❌ 预处理失败: {e}")
        return False

if __name__ == "__main__":
    success = preprocess_json_for_pig()
    if not success:
        print("❌ 预处理失败，请检查HDFS和JSON文件")
        exit(1)