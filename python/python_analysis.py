# python_analysis_fixed_json.py - 修复JSON序列化问题的版本

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, f1_score
import time
import json
import warnings
import os
import multiprocessing as mp
from joblib import Parallel, delayed
from datetime import datetime
import subprocess
warnings.filterwarnings('ignore')

# 动态导入可选依赖
try:
    from xgboost import XGBClassifier
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

# 必需的机器学习模型
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.svm import SVC
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier

class FixedJSONMLAnalysis:
    def __init__(self):
        self.results = []
        self.parallel_results = {}
        self.models_config = {}
        self.data_info = {}
        
        # 固定的HDFS路径 - 必须从这里获取真实数据
        self.hdfs_path = "/user/hadoop/image_analysis/extracted_features_final"
        
        # 固定的核心配置 - 用户要求的性能测试
        self.core_configs = [1, 2, 4, 6]
        
        print("🔧 配置信息:")
        print(f"   HDFS路径: {self.hdfs_path}")
        print(f"   测试核心: {self.core_configs}")
    
    def convert_numpy_types(self, obj):
        """递归转换NumPy类型为Python原生类型，解决JSON序列化问题"""
        if isinstance(obj, (np.integer, np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {self.convert_numpy_types(k): self.convert_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self.convert_numpy_types(item) for item in obj]
        else:
            return obj
    
    def validate_hdfs_connection(self):
        """验证HDFS连接和数据存在"""
        print("🔗 验证HDFS连接...")
        
        try:
            # 检查HDFS服务是否运行
            check_hdfs = subprocess.run(["hdfs", "dfsadmin", "-report"], 
                                      capture_output=True, text=True, timeout=10)
            if check_hdfs.returncode != 0:
                raise Exception(f"HDFS服务不可用: {check_hdfs.stderr}")
            
            print("✅ HDFS服务运行正常")
            
            # 检查特征数据路径是否存在
            check_path = subprocess.run(["hdfs", "dfs", "-test", "-e", self.hdfs_path], 
                                      capture_output=True)
            if check_path.returncode != 0:
                raise Exception(f"HDFS路径不存在: {self.hdfs_path}")
            
            print("✅ HDFS特征数据路径存在")
            
            # 检查路径下是否有数据文件
            check_files = subprocess.run(["hdfs", "dfs", "-count", self.hdfs_path], 
                                       capture_output=True, text=True)
            if check_files.returncode == 0:
                file_count = check_files.stdout.strip().split()[1]
                print(f"✅ HDFS数据文件数: {file_count}")
            else:
                raise Exception("无法统计HDFS文件")
                
            return True
            
        except subprocess.TimeoutExpired:
            raise Exception("HDFS连接超时")
        except Exception as e:
            raise Exception(f"HDFS验证失败: {e}")
    
    def load_real_data_from_hdfs(self):
        """从HDFS加载真实的Spark特征数据"""
        print("📁 从HDFS加载真实特征数据...")
        print(f"📂 数据路径: {self.hdfs_path}")
        
        try:
            # 执行HDFS命令获取数据
            cmd = ["hdfs", "dfs", "-cat", f"{self.hdfs_path}/part-*"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                raise Exception(f"HDFS数据读取失败: {result.stderr}")
            
            lines = result.stdout.strip().split('\n')
            if not lines or lines[0] == '':
                raise Exception("HDFS数据为空")
            
            print(f"✅ 成功读取 {len(lines)} 行真实数据")
            
            return self._parse_real_spark_data(lines)
            
        except subprocess.TimeoutExpired:
            raise Exception("HDFS数据读取超时")
        except Exception as e:
            raise Exception(f"数据加载失败: {e}")
    
    def _parse_real_spark_data(self, lines):
        """解析真实的Spark特征数据"""
        print("🔍 解析真实Spark特征数据...")
        
        features_list = []
        labels_list = []
        valid_count = 0
        error_count = 0
        
        for i, line in enumerate(lines):
            try:
                data = json.loads(line.strip())
                features = data.get('image_features', [])
                label = data.get('label', -1)
                
                # 严格验证数据格式
                if (features and 
                    label in [0, 1] and  # 必须是0或1
                    len(features) > 10 and  # 特征数量必须合理
                    isinstance(features, list)):
                    
                    features_list.append(features)
                    labels_list.append(label)
                    valid_count += 1
                else:
                    error_count += 1
                    
            except (json.JSONDecodeError, TypeError) as e:
                error_count += 1
                if i < 3:  # 只显示前几个错误的详细信息
                    print(f"   ⚠️ 数据解析错误行 {i}: {e}")
        
        print(f"📊 数据质量报告:")
        print(f"   ✅ 有效数据: {valid_count} 行")
        print(f"   ❌ 无效数据: {error_count} 行")
        print(f"   📈 数据质量: {valid_count/(valid_count+error_count)*100:.1f}%")
        
        if valid_count == 0:
            raise Exception("没有有效的特征数据，请检查Spark特征提取是否正确运行")
        
        if valid_count < 10:
            print("⚠️  警告: 有效数据量较少，可能影响模型性能")
        
        X = np.array(features_list)
        y = np.array(labels_list)
        
        # 数据统计信息 - 确保使用Python原生类型
        class_dist = dict(zip(*np.unique(y, return_counts=True)))
        class_dist_native = {int(k): int(v) for k, v in class_dist.items()}  # 转换为原生类型
        
        self.data_info = {
            'samples': int(X.shape[0]),  # 转换为int
            'features': int(X.shape[1]),  # 转换为int
            'class_distribution': class_dist_native,  # 使用转换后的字典
            'feature_range': [float(X.min()), float(X.max())],  # 转换为float
            'feature_mean': float(X.mean()),  # 转换为float
            'data_source': 'Spark_HDFS_Real_Data',
            'load_time': datetime.now().isoformat()
        }
        
        print(f"🎯 真实数据维度: {X.shape[0]} 样本, {X.shape[1]} 特征")
        print(f"📊 类别分布: {self.data_info['class_distribution']}")
        print(f"📈 特征范围: [{self.data_info['feature_range'][0]:.2f}, {self.data_info['feature_range'][1]:.2f}]")
        print(f"📈 特征均值: {self.data_info['feature_mean']:.2f}")
        
        return X, y
    
    def prepare_models(self):
        """准备机器学习模型 - 保持原有配置"""
        print("🤖 初始化机器学习模型...")
        
        # 保持原有的模型配置
        self.models_config = {
            'Random Forest': {
                'model': RandomForestClassifier(n_estimators=100, random_state=42),
                'needs_scaling': False
            },
            'Logistic Regression': {
                'model': LogisticRegression(max_iter=1000, random_state=42),
                'needs_scaling': True
            },
            'Decision Tree': {
                'model': DecisionTreeClassifier(random_state=42),
                'needs_scaling': False
            },
            'SVM': {
                'model': SVC(probability=True, random_state=42),
                'needs_scaling': True
            },
            'Naive Bayes': {
                'model': GaussianNB(),
                'needs_scaling': False
            },
            'K-Neighbors': {
                'model': KNeighborsClassifier(n_neighbors=5),
                'needs_scaling': True
            },
            'Neural Network': {
                'model': MLPClassifier(hidden_layer_sizes=(50,), max_iter=1000, random_state=42),
                'needs_scaling': True
            }
        }
        
        # 如果XGBoost可用，添加它
        if XGBOOST_AVAILABLE:
            self.models_config['XGBoost'] = {
                'model': XGBClassifier(n_estimators=100, random_state=42, verbosity=0),
                'needs_scaling': False
            }
            print("   ✅ XGBoost")
        else:
            print("   ❌ XGBoost (不可用)")
        
        print(f"✅ 初始化了 {len(self.models_config)} 个模型")
        return self.models_config
    
    def train_single_model(self, model_name, model_config, X_train, X_test, y_train, y_test):
        """训练单个模型"""
        try:
            model = model_config['model']
            needs_scaling = model_config['needs_scaling']
            start_time = time.time()
            
            # 训练模型
            if needs_scaling:
                scaler = StandardScaler()
                X_train_scaled = scaler.fit_transform(X_train)
                X_test_scaled = scaler.transform(X_test)
                model.fit(X_train_scaled, y_train)
                y_pred = model.predict(X_test_scaled)
            else:
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
            
            training_time = time.time() - start_time
            
            # 评估模型
            accuracy = accuracy_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred, average='weighted')
            
            return {
                'model_name': model_name,
                'accuracy': float(accuracy),  # 转换为float
                'f1_score': float(f1),  # 转换为float
                'training_time': float(training_time),  # 转换为float
                'success': True
            }
            
        except Exception as e:
            print(f"❌ {model_name} 训练失败: {e}")
            return {
                'model_name': model_name,
                'accuracy': 0.0,
                'f1_score': 0.0,
                'training_time': 0.0,
                'success': False,
                'error': str(e)
            }
    
    def run_sequential_training(self, X, y):
        """顺序训练（单核心基准）"""
        print("🔢 顺序训练（单核心基准）...")
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.3, random_state=42, stratify=y
        )
        
        results = []
        total_start = time.time()
        
        for model_name, model_config in self.models_config.items():
            result = self.train_single_model(
                model_name, model_config, X_train, X_test, y_train, y_test
            )
            results.append(result)
            
            if result['success']:
                print(f"   ✅ {model_name}: 准确率={result['accuracy']:.4f}, 时间={result['training_time']:.2f}s")
        
        total_time = time.time() - total_start
        
        return {
            'results': results,
            'total_time': float(total_time),  # 转换为float
            'cores': 1,
            'method': 'sequential'
        }
    
    def run_parallel_training(self, X, y, n_cores):
        """并行训练"""
        print(f"🔄 并行训练 ({n_cores} 核心)...")
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.3, random_state=42, stratify=y
        )
        
        total_start = time.time()
        
        # 并行训练所有模型
        parallel_results = Parallel(n_jobs=n_cores)(
            delayed(self.train_single_model)(
                model_name, model_config, X_train, X_test, y_train, y_test
            )
            for model_name, model_config in self.models_config.items()
        )
        
        total_time = time.time() - total_start
        
        # 显示结果
        for result in parallel_results:
            if result['success']:
                print(f"   ✅ {result['model_name']}: 准确率={result['accuracy']:.4f}")
        
        return {
            'results': parallel_results,
            'total_time': float(total_time),  # 转换为float
            'cores': n_cores,
            'method': f'parallel_{n_cores}cores'
        }
    
    def run_core_performance_comparison(self, X, y):
        """运行核心数性能比较"""
        print("⚡ 开始多核心性能比较...")
        print("=" * 60)
        
        # 使用固定的核心配置
        max_system_cores = mp.cpu_count()
        actual_cores = [c for c in self.core_configs if c <= max_system_cores]
        
        print(f"💻 系统最大核心数: {max_system_cores}")
        print(f"🔧 实际测试核心: {actual_cores}")
        
        if not actual_cores:
            raise Exception(f"系统只有 {max_system_cores} 核心，无法测试配置 {self.core_configs}")
        
        comparison_results = {}
        
        for n_cores in actual_cores:
            print(f"\n🎯 测试 {n_cores} 核心性能...")
            
            if n_cores == 1:
                result = self.run_sequential_training(X, y)
            else:
                result = self.run_parallel_training(X, y, n_cores)
            
            comparison_results[f'{n_cores}_cores'] = result
            
            # 性能摘要
            successful_models = [r for r in result['results'] if r['success']]
            if successful_models:
                avg_accuracy = np.mean([r['accuracy'] for r in successful_models])
                print(f"   📊 平均准确率: {avg_accuracy:.4f}")
                print(f"   ⏱️  总训练时间: {result['total_time']:.2f}s")
        
        self.parallel_results = comparison_results
        return comparison_results
    
    def calculate_performance_metrics(self):
        """计算性能指标"""
        print("\n📈 计算性能指标...")
        
        if not self.parallel_results:
            return None
        
        sequential_result = self.parallel_results.get('1_cores')
        if not sequential_result:
            return None
        
        sequential_time = sequential_result['total_time']
        metrics = {}
        
        for config, result in self.parallel_results.items():
            if config == '1_cores':
                metrics[config] = {
                    'speedup': 1.0,
                    'efficiency': 1.0,
                    'total_time': float(result['total_time'])  # 转换为float
                }
            else:
                parallel_time = result['total_time']
                n_cores = result['cores']
                speedup = sequential_time / parallel_time if parallel_time > 0 else 1.0
                efficiency = speedup / n_cores
                
                metrics[config] = {
                    'speedup': float(speedup),  # 转换为float
                    'efficiency': float(efficiency),  # 转换为float
                    'total_time': float(parallel_time)  # 转换为float
                }
        
        return metrics
    
    def create_performance_visualizations(self, performance_metrics):
        """创建性能可视化"""
        print("\n🎨 生成性能可视化图表...")
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('真实数据 - Python并行机器学习性能分析', fontsize=16, fontweight='bold')
        
        # 1. 模型准确率比较
        if self.parallel_results:
            sequential_results = self.parallel_results.get('1_cores', {}).get('results', [])
            successful_models = [r for r in sequential_results if r['success']]
            
            if successful_models:
                model_names = [r['model_name'] for r in successful_models]
                accuracies = [r['accuracy'] for r in successful_models]
                
                bars = axes[0, 0].bar(model_names, accuracies, color='skyblue', alpha=0.7)
                axes[0, 0].set_title('模型准确率比较 (真实数据)')
                axes[0, 0].set_ylabel('准确率')
                axes[0, 0].tick_params(axis='x', rotation=45)
                
                # 在柱子上显示数值
                for bar, accuracy in zip(bars, accuracies):
                    height = bar.get_height()
                    axes[0, 0].text(bar.get_x() + bar.get_width()/2., height + 0.01,
                                   f'{accuracy:.3f}', ha='center', va='bottom', fontsize=8)
        
        # 2. 并行加速比
        if performance_metrics:
            configs = list(performance_metrics.keys())
            speedups = [metrics['speedup'] for metrics in performance_metrics.values()]
            
            bars = axes[0, 1].bar(configs, speedups, color='lightgreen', alpha=0.7)
            axes[0, 1].set_title('并行加速比')
            axes[0, 1].set_ylabel('加速比 (倍数)')
            axes[0, 1].tick_params(axis='x', rotation=45)
            
            for bar, speedup in zip(bars, speedups):
                height = bar.get_height()
                axes[0, 1].text(bar.get_x() + bar.get_width()/2., height + 0.1,
                               f'{speedup:.2f}x', ha='center', va='bottom')
        
        # 3. 训练时间比较
        if self.parallel_results:
            configs = list(self.parallel_results.keys())
            times = [result['total_time'] for result in self.parallel_results.values()]
            
            bars = axes[1, 0].bar(configs, times, color='lightcoral', alpha=0.7)
            axes[1, 0].set_title('总训练时间比较')
            axes[1, 0].set_ylabel('时间 (秒)')
            axes[1, 0].tick_params(axis='x', rotation=45)
            
            for bar, time_val in zip(bars, times):
                height = bar.get_height()
                axes[1, 0].text(bar.get_x() + bar.get_width()/2., height + 0.1,
                               f'{time_val:.1f}s', ha='center', va='bottom', fontsize=8)
        
        # 4. 数据信息和配置
        info_text = f"数据信息:\n"
        info_text += f"来源: {self.data_info['data_source']}\n"
        info_text += f"样本数: {self.data_info['samples']}\n"
        info_text += f"特征数: {self.data_info['features']}\n"
        info_text += f"类别分布: {self.data_info['class_distribution']}\n"
        info_text += f"特征范围: [{self.data_info['feature_range'][0]:.1f}, {self.data_info['feature_range'][1]:.1f}]\n\n"
        info_text += f"配置信息:\n"
        info_text += f"测试核心: {self.core_configs}\n"
        info_text += f"模型数量: {len(self.models_config)}\n"
        info_text += f"数据路径: {self.hdfs_path}"
        
        axes[1, 1].text(0.05, 0.95, info_text, fontsize=9, va='top', linespacing=1.5)
        axes[1, 1].set_title('数据和配置信息')
        axes[1, 1].axis('off')
        
        plt.tight_layout()
        plt.savefig('real_data_performance_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("✅ 性能可视化图表已保存: real_data_performance_analysis.png")
    
    def save_real_data_results(self):
        """保存真实数据的结果 - 修复JSON序列化问题"""
        print("\n💾 保存真实数据分析结果...")
        
        # 创建结果目录
        os.makedirs('real_data_results', exist_ok=True)
        
        # 计算性能指标
        performance_metrics = self.calculate_performance_metrics()
        
        # 准备结果数据，确保所有数据类型都是JSON可序列化的
        results_data = {
            'analysis_info': {
                'timestamp': datetime.now().isoformat(),
                'data_source': 'Spark_HDFS_Real_Data',
                'hdfs_path': self.hdfs_path,
                'core_configs': self.core_configs
            },
            'data_info': self.data_info,  # 已经在初始化时转换为原生类型
            'parallel_results': self.convert_numpy_types(self.parallel_results),  # 递归转换
            'performance_metrics': self.convert_numpy_types(performance_metrics)  # 递归转换
        }
        
        # 保存JSON结果
        results_path = 'real_data_results/real_data_analysis.json'
        try:
            with open(results_path, 'w') as f:
                json.dump(results_data, f, indent=2, ensure_ascii=False)
            print(f"✅ JSON结果已保存: {results_path}")
        except Exception as e:
            print(f"❌ JSON保存失败: {e}")
            # 尝试简化保存
            self._save_simplified_results()
            return
        
        # 保存CSV格式的结果
        if self.parallel_results:
            all_results = []
            for config, result in self.parallel_results.items():
                for model_result in result['results']:
                    if model_result['success']:
                        all_results.append({
                            'config': config,
                            'model': model_result['model_name'],
                            'accuracy': float(model_result['accuracy']),
                            'f1_score': float(model_result['f1_score']),
                            'training_time': float(model_result['training_time'])
                        })
            
            df = pd.DataFrame(all_results)
            csv_path = 'real_data_results/model_performance.csv'
            df.to_csv(csv_path, index=False)
            print(f"✅ CSV结果已保存: {csv_path}")
        
        print("✅ 所有结果文件保存完成!")
    
    def _save_simplified_results(self):
        """简化保存结果，确保一定能保存"""
        print("🔄 尝试简化保存结果...")
        
        simplified_data = {
            'timestamp': datetime.now().isoformat(),
            'data_samples': self.data_info['samples'],
            'data_features': self.data_info['features'],
            'data_source': self.data_info['data_source'],
            'models_tested': list(self.models_config.keys())
        }
        
        # 保存核心性能结果
        if self.parallel_results:
            performance_summary = {}
            for config, result in self.parallel_results.items():
                successful_models = [r for r in result['results'] if r['success']]
                if successful_models:
                    avg_accuracy = float(np.mean([r['accuracy'] for r in successful_models]))
                    performance_summary[config] = {
                        'avg_accuracy': avg_accuracy,
                        'total_time': float(result['total_time']),
                        'successful_models': len(successful_models)
                    }
            
            simplified_data['performance_summary'] = performance_summary
        
        # 保存简化结果
        simple_path = 'real_data_results/simplified_results.json'
        with open(simple_path, 'w') as f:
            json.dump(simplified_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ 简化结果已保存: {simple_path}")
    
    def print_final_summary(self):
        """打印最终总结"""
        print("\n" + "="*60)
        print("🏆 真实数据分析总结")
        print("="*60)
        
        print(f"📊 数据信息:")
        print(f"   数据来源: {self.data_info['data_source']}")
        print(f"   HDFS路径: {self.hdfs_path}")
        print(f"   样本数量: {self.data_info['samples']}")
        print(f"   特征维度: {self.data_info['features']}")
        print(f"   类别分布: {self.data_info['class_distribution']}")
        
        # 性能总结
        metrics = self.calculate_performance_metrics()
        if metrics:
            print(f"\n⚡ 性能总结:")
            best_config = max([(k, v) for k, v in metrics.items() if k != '1_cores'], 
                            key=lambda x: x[1]['speedup'], default=None)
            if best_config:
                print(f"   最佳并行配置: {best_config[0]}")
                print(f"   加速比: {best_config[1]['speedup']:.2f}x")
                print(f"   并行效率: {best_config[1]['efficiency']:.2f}")
            
            sequential_time = metrics.get('1_cores', {}).get('total_time', 0)
            print(f"   顺序训练时间: {sequential_time:.2f}s")
        
        # 最佳模型
        if self.parallel_results:
            sequential_results = self.parallel_results.get('1_cores', {}).get('results', [])
            successful_models = [r for r in sequential_results if r['success']]
            if successful_models:
                best_model = max(successful_models, key=lambda x: x['accuracy'])
                print(f"\n🎯 最佳模型: {best_model['model_name']}")
                print(f"   准确率: {best_model['accuracy']:.4f}")
                print(f"   F1分数: {best_model['f1_score']:.4f}")
                print(f"   训练时间: {best_model['training_time']:.2f}s")

def main():
    """主函数"""
    print("=" * 60)
    print("🚀 真实数据Python并行机器学习分析 (修复JSON版本)")
    print("=" * 60)
    print("💡 此版本修复了JSON序列化问题，强制使用真实Spark特征数据")
    print("=" * 60)
    
    # 初始化分析器
    analyzer = FixedJSONMLAnalysis()
    
    try:
        # 1. 验证HDFS连接
        analyzer.validate_hdfs_connection()
        
        # 2. 加载真实数据
        X, y = analyzer.load_real_data_from_hdfs()
        
        # 3. 准备模型
        analyzer.prepare_models()
        
        # 4. 运行核心性能比较
        analyzer.run_core_performance_comparison(X, y)
        
        # 5. 计算性能指标
        performance_metrics = analyzer.calculate_performance_metrics()
        
        # 6. 创建可视化
        analyzer.create_performance_visualizations(performance_metrics)
        
        # 7. 保存结果
        analyzer.save_real_data_results()
        
        # 8. 打印总结
        analyzer.print_final_summary()
        
        print("\n✅ 真实数据分析完成!")
        print("📁 所有结果保存在 'real_data_results/' 目录中")
        
    except Exception as e:
        print(f"\n❌ 分析失败: {e}")
        print("💡 请确保:")
        print("   1. Hadoop服务正在运行")
        print("   2. Spark特征提取已完成")
        print("   3. HDFS路径存在: /user/hadoop/image_analysis/extracted_features_final")
        raise

if __name__ == "__main__":
    main()