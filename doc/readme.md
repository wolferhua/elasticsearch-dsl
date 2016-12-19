### select
*   ['id', 'district', 'block', 'citycode']

### where
#### 可以使用两种方式：
*    哈希格式，例如： ['column1' => value1, 'column2' => value2, ...]
*    操作符格式，例如：[operator,operand1, operand2, ...]

##### 哈希格式
*   ['type' => 1, 'status' => 2]
*   ['id' => [1, 2, 3], 'status' => 2]
*   ['status' => null]

##### 操作符格式
*   [操作符, 操作数1, 操作数2, ...]
*   ['and', ['district' => 1337], ['block' => 1337]] 
*   ['and', ['district' => 1337], ['or', ['block' => 1338], ['block' => 1339]]] 
*   ['or', ['type' => [7, 8, 9]], ['id' => [1, 2, 3]]
*   ['not', ['attribute' => null]]
*   ['between', 'id', 1, 10]
*   ['in', 'id', [1, 2, 3]]
*   ['in', ['id', 'name'], [['id' => 1, 'name' => 'foo'], ['id' => 2, 'name' => 'bar']] ]
*   ['like', 'name', 'tester']
*   ['like', 'name', ['test', 'sample']]
*   ['>=', 'id', 10]

### group by
*   ['block','citycode']
*   ['block'=>['offset'=>1,'limit'=>2,'field'=>['district','block']]]
*   ['block'=>['offset'=>1,'limit'=>2,'field'=>['district','block']],'citycode']

### order by
*   ['id' => 'desc']
