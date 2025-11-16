-- data_cleaning_with_images_json_fixed.pig - 最终修复版

-- =============================================
-- 第一步：清理可能存在的输出目录
-- =============================================
rmf /user/hadoop/image_analysis/cleaned_data_with_images_json_fixed;
rmf /user/hadoop/image_analysis/category_stats_json_fixed;
rmf /user/hadoop/image_analysis/keyword_stats_json_fixed;

-- =============================================
-- 第二步：数据加载和预处理
-- =============================================
raw_text = LOAD '/user/hadoop/image_analysis/image_metadata_line_by_line.json' 
    USING TextLoader() AS (line:chararray);

-- 显示数据量
data_count = FOREACH (GROUP raw_text ALL) GENERATE COUNT(raw_text) AS total_lines;
DUMP data_count;

-- =============================================
-- 第三步：提取字段 - 最终修复！
-- =============================================
-- 方法1: 使用更精确的正则表达式匹配完整的多维数组
extracted_data = FOREACH raw_text GENERATE
    REGEX_EXTRACT(line, '"image_id":\\s*"([^"]*)"', 1) AS image_id,
    REGEX_EXTRACT(line, '"label":\\s*(\\d+)', 1) AS label,
    REGEX_EXTRACT(line, '"keyword":\\s*"([^"]*)"', 1) AS keyword,
    REGEX_EXTRACT(line, '"tags":\\s*"([^"]*)"', 1) AS tags,
    REGEX_EXTRACT(line, '"image_array":\\s*(\\[\\s*\\[\\s*\\[.*?\\]\\s*\\]\\s*\\])', 1) AS image_array,
    REGEX_EXTRACT(line, '"original_shape":\\s*(\\[\\d+,\\s*\\d+,\\s*\\d+\\])', 1) AS original_shape;

-- 显示提取的样本数据（检查图像数组长度）
sample_check = FOREACH (LIMIT extracted_data 5) GENERATE
    image_id,
    SIZE(image_array) AS array_length;
DUMP sample_check;

-- =============================================
-- 第四步：数据清洗和验证
-- =============================================
-- 更严格的过滤条件
cleaned_data = FILTER extracted_data BY
    image_id IS NOT NULL AND
    image_id != '' AND
    label IS NOT NULL AND
    keyword IS NOT NULL AND
    image_array IS NOT NULL AND
    image_array != '[]' AND
    image_array != '[[[]]]' AND
    SIZE(image_array) > 1000;  -- 确保有足够的图像数据

-- 转换label为整数
final_data = FOREACH cleaned_data GENERATE
    image_id,
    (int)label AS label,
    keyword,
    tags,
    image_array,
    original_shape;

-- 验证标签有效性
valid_data = FILTER final_data BY (label == 0 OR label == 1);

-- 显示有效数据量
valid_count = FOREACH (GROUP valid_data ALL) GENERATE COUNT(valid_data) AS valid_records;
DUMP valid_count;

-- =============================================
-- 第五步：统计分析
-- =============================================
category_stats = FOREACH (GROUP valid_data BY label) GENERATE
    group AS label,
    COUNT(valid_data) AS count;
DUMP category_stats;

keyword_stats = FOREACH (GROUP valid_data BY keyword) GENERATE
    group AS keyword,
    COUNT(valid_data) AS count;
DUMP keyword_stats;

-- =============================================
-- 第六步：存储结果 - 使用JSON格式！
-- =============================================
-- 存储清洗后的数据（使用JsonStorage）
STORE valid_data INTO '/user/hadoop/image_analysis/cleaned_data_with_images_json_fixed' USING JsonStorage();

-- 存储统计结果（也使用JSON格式）
STORE category_stats INTO '/user/hadoop/image_analysis/category_stats_json_fixed' USING JsonStorage();
STORE keyword_stats INTO '/user/hadoop/image_analysis/keyword_stats_json_fixed' USING JsonStorage();

-- 显示完成信息
final_summary = FOREACH (GROUP valid_data ALL) GENERATE
    CONCAT('最终修复版完成！有效记录数: ', (chararray)COUNT(valid_data)) AS summary;
DUMP final_summary;