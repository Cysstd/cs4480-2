#!/bin/bash
echo "🐧 WSL2完整大数据环境安装脚本"
echo "=========================================="

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 1. 安装Java
log_info "1. 安装Java 11..."
sudo apt install -y openjdk-11-jdk openjdk-11-jre
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc
source ~/.bashrc

# 2. 安装Hadoop
log_info "2. 安装Hadoop 3.3.1..."
cd ~
wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
tar -xzf hadoop-3.3.1.tar.gz
sudo mv hadoop-3.3.1 /usr/local/hadoop
rm hadoop-3.3.1.tar.gz

# 设置Hadoop环境变量
echo "export HADOOP_HOME=/usr/local/hadoop" >> ~/.bashrc
echo "export HADOOP_MAPRED_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_COMMON_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_HDFS_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export YARN_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native" >> ~/.bashrc
echo "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin" >> ~/.bashrc

# 3. 安装Spark
log_info "3. 安装Spark 3.1.2..."
wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
tar -xzf spark-3.1.2-bin-hadoop3.2.tgz
sudo mv spark-3.1.2-bin-hadoop3.2 /usr/local/spark
rm spark-3.1.2-bin-hadoop3.2.tgz

# 设置Spark环境变量
echo "export SPARK_HOME=/usr/local/spark" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> ~/.bashrc
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc

# 4. 安装Pig
log_info "4. 安装Pig 0.17.0..."
wget -q https://archive.apache.org/dist/pig/pig-0.17.0/pig-0.17.0.tar.gz
tar -xzf pig-0.17.0.tar.gz
sudo mv pig-0.17.0 /usr/local/pig
rm pig-0.17.0.tar.gz

# 设置Pig环境变量
echo "export PIG_HOME=/usr/local/pig" >> ~/.bashrc
echo "export PATH=\$PATH:\$PIG_HOME/bin" >> ~/.bashrc
echo "export PIG_CLASSPATH=\$HADOOP_HOME/etc/hadoop" >> ~/.bashrc

# 5. 安装R
log_info "5. 安装R..."
sudo apt install -y r-base r-base-dev

# 6. 安装Python依赖
log_info "6. 安装Python依赖..."
sudo apt install -y python3 python3-pip python3-venv
pip3 install requests pillow numpy scikit-image scikit-learn matplotlib seaborn pandas

# 重新加载环境变量
source ~/.bashrc

log_info "安装完成！"
echo ""
log_info "环境变量已设置："
echo "JAVA_HOME: $JAVA_HOME"
echo "HADOOP_HOME: /usr/local/hadoop"
echo "SPARK_HOME: /usr/local/spark"
echo "PIG_HOME: /usr/local/pig"
