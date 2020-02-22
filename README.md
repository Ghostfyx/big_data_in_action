# 大数据体系实战项目
将理论应用与实际才是真正有收获的
## 1. Storm
### 案例1 词频统计
这里使用自定义的 DataSourceSpout 产生词频数据，然后使用自定义的 SplitBolt 和 CountBolt 来进行词频统计。

![](https://github.com/heibaiying/BigData-Notes/raw/master/pictures/storm-word-count-p.png)