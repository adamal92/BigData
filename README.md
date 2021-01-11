# BigData
This is an attempt to create a basic library for Big Data in python

# Plan
web crawler -> cluster -> map-reduce -> NoSQL -> visualization

# Execution
scrapy -> HDFS -> spark -> elasticsearch -> js react client

TODO: web crawler (scrapy) -> cluster (HDFS) -> map-reduce (spark) -> NoSQL (elasticsearch) -> SQL (SQLite) -> visualization (matplotlib)

# Projects

----------------
## moto prices 
<h3> Pseudo Code </h3>

```python
for site in sites_list:
    for div_element:
        recurse()
    if div_element is None:
    for html_element.text():
        type = filter/diagnose(element)
        sql.insert("INSERT VALUES(type element);")
HDFS.save_file(moto_list.db)
json = Spark.process(HDFS.get(moto_list.db))
Elastic.save(json)
react.fetch(json).visualize()
```

<h1> tasks </h1>

- [ ] לרוץ על כל span
- [ ] לפלטר לפי הערך (גם אם מלוכלך)
- [x] להכניס ל sql לפי הפילטר
- [x] לשמור את ה sql ב HDFS

* site with moto prices
* scrape model & prices
* save to HDFS
* map-reduce/process & mine/analyze/(ML?)
* save to elastic
* Flask
* visualize in react

---------------------------

model => range of prices


index | year | cc | price | color | model
--- | --- | --- | --- | --- | ---
1 | 2002 | 400 | 200$ | #FFF | kawasaki
2 | 2003 | 200 | | | ninja
3 | 2002 | 400 | | white | 
