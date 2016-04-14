# Example

我会在这里简单的介绍几种使用ETS(gen_rets)的模式, 也仅当做是我自己的总结.

## query_cache

query_cache 是一个为查询数据库做缓存的组件, 可以是MySQL 数据库，PG或者是Mongo 之类的，这些和query_cache 并没有关系.

为了界定问题域, 首先做几个假定. 

* 主库中的数据, 一次写入多次读取.
* query_cache 仅限于写入和删除

## TODO ...
