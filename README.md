# 搜索日志查询与统计分析系统

基于HBase和Spark的用户搜索日志查询与统计分析系统，严格按照项目要求实现所有功能。

## 目录

- [系统要求](#系统要求)
- [项目结构](#项目结构)
- [数据格式](#数据格式)
- [功能实现](#功能实现)
  - [基于行键的搜索](#一基于行键的搜索)
  - [HBase API条件查询功能](#二hbase-api条件查询功能)
  - [Spark统计分析功能](#三spark统计分析功能)
  - [数据加载功能](#四数据加载功能)
  - [数据管理功能](#五数据管理功能)
  - [MongoDB数据加载功能（可选）](#六mongodb数据加载功能可选)
  - [图表生成功能（可选）](#七图表生成功能可选)
- [快速开始](#快速开始)
- [命令行参数说明](#命令行参数说明)
- [功能示例](#功能示例)
- [技术实现要点](#技术实现要点)
- [注意事项](#注意事项)


## 系统要求
- Java 8+
- Hadoop 3.3.6 (单节点，仅客户端依赖)
- HBase 2.5.13 (单节点，需要独立安装运行)
- Spark 3.5.7 (用于统计分析，本地模式)
- Gradle 7+ (用于构建)
- Redis 7+ (可选，用于查询缓存)

## 项目结构
```
src/main/java/com/sohu/logs/
├── MainApplication.java          # 主程序入口
├── DataLoader.java               # 数据加载器（从文件到HBase）
├── model/                        # 数据模型
│   └── LogRecord.java            # 日志记录实体类
├── util/                         # 工具类
│   ├── TimeUtils.java            # 时间处理工具
│   ├── DomainUtils.java          # 域名提取工具
│   └── TaskLogger.java           # 任务日志记录器
├── search/                       # HBase搜索功能核心
│   ├── SearchCondition.java      # 搜索条件解析
│   ├── SearchEngine.java         # HBase查询引擎
│   └── RedisCache.java           # Redis缓存支持
├── hbase/                        # HBase辅助类
│   ├── HBaseSchemaCreator.java   # 表结构创建
│   ├── HBaseWriter.java          # 批量写入器
│   └── HBaseCleaner.java         # 数据表清理
├── spark/                        # Spark统计分析
│   └── SparkAnalysisService.java # Spark分析服务
├── service/                      # 业务服务层
│   ├── SearchService.java        # 搜索服务
│   ├── DataLoadService.java      # 数据加载服务
│   └── SparkSearchService.java   # Spark搜索服务
├── cli/                          # 命令行界面
│   ├── CommandLineParser.java     # 命令行参数解析
│   └── MenuHandler.java          # 菜单处理器
├── config/                       # 配置类
│   └── AppConfig.java            # 应用配置
└── web/                          # Web界面
    └── DashboardController.java  # Web控制器

src/main/resources/
└── static/                       # 静态资源文件
    └── index.html                # Web Dashboard页面
```

## 数据格式
原始日志文件格式（制表符分隔）：
```
访问时间\t用户ID\t[查询词]\t排名 点击顺序\tURL
示例：
00:00:01\tuser1\t[旅游]\t1 1\thttp://www.sohu.com/travel
```
**字段说明**：
1. **访问时间**：格式HH:mm:ss（24小时制），仅时间部分，程序会自动使用当天日期
2. **用户ID**：用户标识符
3. **查询词**：搜索关键词，用方括号括起
4. **排名 点击顺序**：两个整数，空格分隔（排名和点击顺序）
5. **URL**：用户点击的完整URL地址

**注意**：数据通过 `load` 命令加载到HBase后，所有查询和分析操作都从HBase读取数据。

## 功能实现

### 一、基于行键的搜索
**行键设计**：`salt(userId) + reverseTimestamp + "#" + userId + "#" + domain`
- `salt(userId)`: 用户ID的哈希分桶（16个桶），保证数据均匀分布
- `reverseTimestamp`: Long.MAX_VALUE - 访问时间戳(毫秒)，优化时间范围查询
- `userId`: 用户ID
- `domain`: URL的一级域名（如sohu、baidu）

**支持的行键搜索**：
- 完整行键精确查询
- 基于时间、用户、域名的前缀搜索
- 支持访问时间、用户ID、网站一级域名中的一个或多个字段组合搜索

### 二、HBase API条件查询功能
支持六个字段的条件查询，可任意组合（所有条件同时满足）：

1. **时间范围查询**：`time:开始时间|结束时间`
2. **用户ID查询**：`user:用户ID1|用户ID2`（支持多个，OR逻辑）
3. **查询关键词搜索**：`query:关键词1|关键词2`（支持多个，OR逻辑）
4. **域名关键词搜索**：`domain:域名1|域名2`（支持多个，OR逻辑）
5. **排名范围查询**：`rank:最小值-最大值`（如rank:1-10）
6. **点击顺序范围查询**：`click:最小值-最大值`（如click:1-5）

**组合查询格式**：`time:... + user:... + query:... + domain:... + rank:... + click:...`

### 三、Spark统计分析功能
从HBase读取数据，使用Spark SQL实现以下统计：

1. **时段流量统计**：输入起始时间和结束时间，统计：
   - 时间段内的总搜索次数
   - 热门查询词搜索次数（前20）
   - 热门网站访问量（按一级域名统计，前20）
2. **用户使用频率统计**：统计每个用户搜索次数排名（前20）
3. **访问行为统计**：根据页面在搜索结果中的排名，统计不同排名的结果被访问情况
4. **点击顺序分析**：统计不同点击顺序的分布
5. **域名分布分析**：统计各域名访问百分比
6. **汇总报告**：总搜索数、唯一用户数、唯一查询词数、唯一域名数

### 四、数据加载功能
支持从原始日志文件加载数据到HBase：
- **批量写入**：可配置批量大小（默认5000），提升写入性能
- **错误处理**：自动跳过格式错误行，记录错误日志
- **编码支持**：支持多种文件编码（默认UTF-8）
- **任务日志**：完整记录加载过程，包括成功/失败统计

### 五、数据管理功能
- **表管理**：自动创建HBase表（如果不存在）
- **数据清空**：安全清空数据表（删除并重建表结构）
- **连接管理**：可配置ZooKeeper连接参数

### 六、MongoDB数据加载功能（可选）
- **MongoDB支持**：支持将数据加载到MongoDB进行存储
- **批量写入**：可配置批量大小（默认5000），提升写入性能
- **错误处理**：自动跳过格式错误行，记录错误日志
- **编码支持**：支持多种文件编码（默认UTF-8）

### 七、图表生成功能（可选）
- **多种图表类型**：支持柱状图、饼图、折线图等多种图表类型
- **智能分组**：数据过多时自动分组显示（前8条+其他）
- **中文支持**：自动处理中文字体，防止乱码
- **灵活配置**：可通过参数控制是否生成图表

## 快速开始

### 1. 环境准备

确保以下服务已安装并运行：

1. **HBase单节点**（必须）
   ```bash
   # 假设HBase已安装，启动HBase服务
   start-hbase.sh
   ```
   - ZooKeeper地址：localhost:2181
   - HBase Master UI：http://localhost:16010
   - 表名：search_logs

2. **Redis**（可选，用于查询缓存）
   ```bash
   # 启动Redis服务
   redis-server
   ```
   - 地址：localhost:6379

3. **MongoDB**（可选，用于MongoDB数据加载）
   ```bash
   # 启动MongoDB服务
   mongod
   ```
   - 地址：localhost:27017
   - 数据库：search_logs
   - 集合：logs

4. **Hadoop HDFS**（可选，仅客户端依赖，无需启动完整集群）

**注意**：本系统使用HBase作为主要数据存储，Spark在本地模式下运行，无需启动Spark集群。

### 2. 构建项目并安装可执行文件
```bash
# 编译项目
./gradlew build

# 安装可执行文件到 build/install/search-log-analyzer/
./gradlew installDist

# 测试构建是否成功
./gradlew run --args="help"
```

### 3. 命令行使用方式

#### 使用可执行文件
```bash
# 直接运行程序启动Web Dashboard并显示交互菜单（默认端口9090）
./build/install/search-log-analyzer/bin/search-log-analyzer

# 显示帮助信息
./build/install/search-log-analyzer/bin/search-log-analyzer help

# 启动Web Dashboard（指定端口）
./build/install/search-log-analyzer/bin/search-log-analyzer web 8080

# 其他命令行功能（可选）
# HBase交互式搜索模式
./build/install/search-log-analyzer/bin/search-log-analyzer search

# 行键精确查询
./build/install/search-log-analyzer/bin/search-log-analyzer rowkey "0_user123#sohu"

# Spark统计分析（从HBase读取数据）
./build/install/search-log-analyzer/bin/search-log-analyzer stats 09:00:00 18:00:00 output_results

# 数据加载到HBase
./build/install/search-log-analyzer/bin/search-log-analyzer load test_log.txt

# 数据加载到MongoDB
./build/install/search-log-analyzer/bin/search-log-analyzer mongoload test_log.txt

# 清空数据表
./build/install/search-log-analyzer/bin/search-log-analyzer clean
```
### 4. 使用Web Dashboard

Web Dashboard提供图形化界面进行系统操作，可以在新的终端窗口中启动：

```bash
# 在新终端窗口中启动Web Dashboard并显示交互菜单（默认端口9090）
./build/install/search-log-analyzer/bin/search-log-analyzer dashboard

# 指定端口在新终端窗口中启动
./build/install/search-log-analyzer/bin/search-log-analyzer dashboard 8080

# 只显示终端交互菜单（不启动Web服务器）
./build/install/search-log-analyzer/bin/search-log-analyzer
```

启动后：
- Web Dashboard将在新的终端窗口中运行，显示服务器日志
- 在浏览器中访问显示的地址即可使用图形化界面
- 同时在原终端中也可以使用交互菜单进行操作

功能包括：
- 系统状态检查
- 行键精确查询
- HBase条件查询
- Spark条件查询
- Spark统计分析
- 数据加载和管理

### 5. 使用HBase查询功能

#### 交互式搜索模式
```bash
./build/install/search-log-analyzer/bin/search-log-analyzer search
```
在交互模式下输入搜索条件：
```
搜索> time:00:00:00|01:00:00 + user:user1 + domain:sohu + rank:1-5
搜索> query:旅游|美食 + domain:baidu + rank:1-3
搜索> user:user1|user2|user3 + time:00:00:00|00:30:00
```

#### 行键精确查询
```bash
./build/install/search-log-analyzer/bin/search-log-analyzer rowkey "0_user123#sohu"
```

### 5. 使用Spark统计分析
```bash
# 基本统计分析（使用默认时间范围 00:00:00-23:59:59）
./build/install/search-log-analyzer/bin/search-log-analyzer stats

# 指定时间范围统计分析
./build/install/search-log-analyzer/bin/search-log-analyzer stats 00:00:00 01:00:00

# 指定时间范围和输出目录
./build/install/search-log-analyzer/bin/search-log-analyzer stats 00:00:00 12:00:00 output_results
```

## 命令行参数说明

程序支持以下命令行参数：

### 无参数
启动Web Dashboard并显示交互式菜单，同时Web服务器在后台运行，提供图形化和命令行两种操作方式

### help / -h / --help
显示帮助信息和使用示例。

### search
进入HBase交互式搜索模式，支持六个字段的条件查询和组合搜索。

### rowkey <行键>
按行键精确查询HBase记录。
- `<行键>`：要查询的完整行键字符串
- 示例：`rowkey "0_user123#sohu"`

### stats [起始时间] [结束时间] [输出目录] [ZooKeeper地址] [ZooKeeper端口] [生成图表]
执行Spark统计分析（从HBase读取数据）。
- `[起始时间]`：统计起始时间，格式HH:mm:ss，默认00:00:00
- `[结束时间]`：统计结束时间，格式HH:mm:ss，默认23:59:59
- `[输出目录]`：结果输出目录，默认output
- `[ZooKeeper地址]`：HBase ZooKeeper地址，默认localhost
- `[ZooKeeper端口]`：HBase ZooKeeper端口，默认2181
- `[生成图表]`：是否生成图表，true/false，默认true
- 示例：`stats 09:00:00 18:00:00 output localhost 2181 true`

### load <数据文件> [ZooKeeper地址] [ZooKeeper端口] [批量大小] [编码]
加载数据文件到HBase。
- `<数据文件>`：原始日志文件路径（必填）
- `[ZooKeeper地址]`：HBase ZooKeeper地址，默认localhost
- `[ZooKeeper端口]`：HBase ZooKeeper端口，默认2181
- `[批量大小]`：批量写入大小，默认5000
- `[编码]`：文件编码，默认UTF-8
- 示例：`load test_log.txt localhost 2181 5000 UTF-8`

### mongoload <数据文件> [MongoDB连接字符串] [批量大小] [编码]
加载数据文件到MongoDB。
- `<数据文件>`：原始日志文件路径（必填）
- `[MongoDB连接字符串]`：MongoDB连接字符串，默认mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.5.10
- `[批量大小]`：批量写入大小，默认5000
- `[编码]`：文件编码，默认UTF-8
- 示例：`mongoload test_log.txt "mongodb://127.0.0.1:27017/" 5000 UTF-8`

### clean [truncate]
清空数据表所有数据。
- `[truncate]`：可选的确认参数，输入"truncate"以执行清空
- 示例：`clean` 或 `clean truncate`

### web [port]
启动Web Dashboard图形化界面。
- `[port]`：可选的端口号，默认9090
- 示例：`web` 或 `web 8080`

### sparksearch <搜索条件> [ZooKeeper地址] [ZooKeeper端口] [是否显示详情]
使用Spark进行条件查询（从HBase读取数据）。
- `<搜索条件>`：搜索条件字符串，格式与HBase条件查询相同
- `[ZooKeeper地址]`：HBase ZooKeeper地址，默认localhost
- `[ZooKeeper端口]`：HBase ZooKeeper端口，默认2181
- `[是否显示详情]`：是否显示详细结果，true/false，默认true
- 示例：`sparksearch "time:00:00:00|01:00:00 + user:user1" localhost 2181 true`

### retry
重试未完成的任务（如数据加载失败的任务）。

## 功能示例

### 完整使用流程示例

#### 1. 数据加载
```bash
# 加载测试数据到HBase
./search-log-analyzer load test_log.txt

# 加载测试数据到MongoDB
./search-log-analyzer mongoload test_log.txt

# 加载自定义文件，指定参数
./search-log-analyzer load /path/to/logs.tsv 192.168.1.100 2181 10000 GBK

# 加载自定义文件到MongoDB，指定参数
./search-log-analyzer mongoload /path/to/logs.tsv "mongodb://192.168.1.100:27017/" 10000 GBK
```

#### 2. HBase条件查询示例
1. **访问时间在1点到2点之间且URL属于百度的网页**：
   ```
   time:01:00:00|02:00:00 + domain:baidu
   ```

2. **用户user1在00:00到00:30之间搜索"旅游"或"美食"的记录**：
   ```
   time:00:00:00|00:30:00 + user:user1 + query:旅游|美食
   ```

3. **排名前5且点击顺序为1的sohu网站记录**：
   ```
   rank:1-5 + click:1-1 + domain:sohu
   ```

4. **复合条件查询（用户ID为user1或user2，排名1-10）**：
   ```
   user:user1|user2 + rank:1-10
   ```

#### 3. 行键精确查询
```bash
# 查询特定行键的记录
./search-log-analyzer rowkey "0_user123#sohu"
```

#### 4. Spark统计分析
```bash
# 分析全天数据
./search-log-analyzer stats

# 分析指定时间范围
./search-log-analyzer stats 09:00:00 18:00:00 work_hours_output

# 分析指定时间范围，自定义HBase连接
./search-log-analyzer stats 00:00:00 12:00:00 morning_stats 192.168.1.100 2181
```

#### 5. Web Dashboard
```bash
# 直接运行程序启动Web Dashboard（默认端口9090）
./search-log-analyzer

# 或者指定自定义端口
./search-log-analyzer web 8080

# 然后在浏览器中访问显示的地址
```

### Spark统计输出
统计分析将生成以下结果文件（按时间戳组织）：
- `yyyyMMdd_HHmmss/query_statistics/` : 查询词搜索次数统计
- `yyyyMMdd_HHmmss/domain_statistics/` : 网站访问量统计（按一级域名）
- `yyyyMMdd_HHmmss/user_frequency/` : 用户使用频率统计
- `yyyyMMdd_HHmmss/rank_statistics/` : 不同排名访问情况统计
- `yyyyMMdd_HHmmss/click_order_statistics/` : 点击顺序统计
- `yyyyMMdd_HHmmss/domain_distribution/` : 域名分布分析（带百分比）
- `yyyyMMdd_HHmmss/summary_report/` : 汇总报告（总搜索数、唯一用户数等）

**图表输出**（当`generateCharts=true`时）：
- `yyyyMMdd_HHmmss/charts/` : 所有生成的图表文件（PNG格式）
  - `top_queries.png` : 热门查询词柱状图
  - `top_domains.png` : 热门网站柱状图
  - `user_frequency.png` : 用户使用频率柱状图
  - `rank_distribution.png` : 排名分布柱状图
  - `click_order_distribution.png` : 点击顺序分布柱状图
  - `domain_distribution_pie.png` : 域名分布饼图

## 技术实现要点

### 1. 行键设计优化
- 反向时间戳：便于时间范围查询，最新的记录在前
- Salt分桶：避免热点问题，数据均匀分布
- 复合键：支持多维度前缀搜索

### 2. HBase查询优化
- 使用FilterList组合多个过滤器
- 支持AND/OR逻辑组合
- 数值范围查询使用GREATER_OR_EQUAL和LESS_OR_EQUAL比较器
- 关键词查询使用SubstringComparator支持子字符串匹配

### 3. Spark统计分析
- 使用DataFrame和Spark SQL进行高效统计
- 支持UTF-8编码，正确处理中文数据
- 实现所有要求的统计功能
- 结果输出为CSV格式，便于后续处理

### 4. Redis缓存优化
- 查询结果缓存：重复查询自动缓存，提升响应速度
- 连接池管理：高效复用Redis连接
- 缓存失效策略：数据变更时自动清除相关缓存

### 5. 任务日志系统
- 操作审计：记录所有数据加载、查询、分析操作
- 性能监控：记录任务执行时间和状态
- 错误追踪：详细记录失败原因和堆栈信息

### 6. 图表生成系统
- 多种图表类型：支持柱状图、饼图、折线图等
- 智能分组：数据过多时自动分组显示（前8条+其他）
- 中文支持：自动处理中文字体，防止乱码
- 灵活配置：可通过参数控制是否生成图表

### 7. Web Dashboard系统
- 轻量级Web框架：使用Javalin提供RESTful API
- 图形化界面：提供直观的Web界面进行系统操作
- 静态资源管理：HTML/CSS/JS文件存放于resources/static目录
- 实时交互：支持搜索、分析、数据管理等功能
- 响应式设计：适配不同屏幕尺寸的设备

### 7. 中文支持
- 所有界面提示信息使用中文
- Spark读取数据时指定UTF-8编码
- 支持中文查询关键词搜索

## 注意事项
1. **HBase服务**：必须启动HBase服务（包括ZooKeeper），默认连接localhost:2181
2. **Redis缓存**（可选）：如需使用缓存功能，请启动Redis服务，默认连接localhost:6379
3. **时间格式**：必须为"HH:mm:ss"（24小时制）
4. **搜索条件**：不区分大小写，组合查询中所有条件必须同时满足（AND逻辑）
5. **数据加载**：加载前确保HBase表存在（程序会自动创建）
6. **Spark分析**：从HBase读取数据，无需原始日志文件
7. **编码支持**：支持UTF-8编码文件，中文数据文件需使用UTF-8编码
8. **推荐使用方式**：
     - 生产环境：使用可执行文件 `./build/install/search-log-analyzer/bin/search-log-analyzer`
9. **数据安全**：清空数据表操作不可逆，请谨慎使用 `clean` 命令