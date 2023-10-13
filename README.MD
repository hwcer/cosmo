仿造GORM接口的MongoDB数据库操作，使用方法参考GORM文档

https://gorm.io/zh_CN/docs/create.html

<font color='red'>查找</font>

db.Find(&struct{},where)

db.Model(&struct{}).Find(&map{},where)

使用Omit排除部分columns 
db.Omit(columns ...string).Find(&struct{},where)

使用Select选定部分columns,只返回选定字段值 
db.Select(columns ...string).Find(&struct{},where)

<font color='red'>更新</font>

更新所有非空字段: db.Update(&struct{},where)

更新MAP中所有值: db.Model(&struct{}).Update(&map{},where)

排除Omit中的字段: db.Omit(columns ...string).Update(&struct{},where)

只更新Select的字段: db.Select(columns ...string).Update(&struct{},where)


<font color='red'>更新并同步内存数据</font>

更新,并将修改后的结果同步更新到Model(&struct{})中,可以配合Omit,Select 排除/选定字段  
db.Model(&struct{},true).Update(&map{}|&struct{},where)