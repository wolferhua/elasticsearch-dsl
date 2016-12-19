## ElasticSearchDsl
```
  _____   _____ _      
 |  __ \ / ____| |     
 | |  | | (___ | |     
 | |  | |\___ \| |     
 | |__| |____) | |____ 
 |_____/|_____/|______|
 ```

### 安装[Composer]

```
composer require xiaobeicn/elasticsearch-dsl
```

### 使用
```php
use Xiaobeicn\ElasticSearchDsl\ElasticSearchDsl;

$dslobj = new ElasticSearchDsl();
$dslobj->select(['id', 'district', 'block', 'citycode'])
        ->where(['district' => 1337])
        ->orderby(['id' => 'desc'])
        ->groupBy(['block' => ['offset' => 1, 'limit' => 2, 'field' => ['district', 'block']], 'citycode'])
        ->limit(2)
        ->offset(1);
$dsl = $dslobj->build();

echo json_encode($dsl);
```

### Doc
[Document](https://github.com/xiaobeicn/elasticsearch-dsl/tree/master/doc)

### License

MIT
