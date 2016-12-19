<?php
namespace Xiaobeicn\ElasticSearchDsl;

use PHPUnit\Framework\TestCase;
use Xiaobeicn\ElasticSearchDsl\ElasticSearchDsl;

class DslTest extends TestCase
{
    public function testQuery() 
    {
        $dslobj = new ElasticSearchDsl();
        $dslobj->select(['id', 'district', 'block', 'citycode'])
        ->where(['district' => 1337])
        ->orderby(['id' => 'desc'])
        ->groupBy(['block' => ['offset' => 1, 'limit' => 2, 'field' => ['district', 'block']], 'citycode'])
        ->limit(2)
        ->offset(1);
        $dsl = $dslobj->build();
        
        echo json_encode($dsl);
    }
}