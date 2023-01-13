仿造GORM接口的MongoDB数据库操作，使用方法参考GORM文档

https://gorm.io/zh_CN/docs/create.html

<font color='red'>查找</font>

db.Find(&struct{},where)

db.Model(&struct{}).Find(&map{},where)

db.Omit(columns ...string).Find(&struct{},where)

db.Select(columns ...string).Find(&struct{},where)

<font color='red'>更新</font>

更新所有非空字段:db.Update(&struct{},where)

更新MAP中所有值:db.Model(&struct{}).Update(&map{},where)

排除Omit和空值:db.Omit(columns ...string).Update(&struct{},where)

只更新Select的字段: db.Select(columns ...string).Update(&struct{},where)


<font color='red'>更新并同步内存数据</font>

db.FindAndUpdate().Select(columns ...string).Update(&struct{},where)

只更新Select的字段,并将修改后的结果同步更新到&struct{}中

db.Model(&struct{}).FindAndUpdate().Select(columns ...string).Update(&map{},where)

更新map中的所有字段,并获取Select中字段的值,然后同步更新到Model中的&struct{}中