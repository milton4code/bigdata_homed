package com.examples

object ZcansTest {



}

 /* def zscan(key: String, cursor: Long, params: ScanParams): ScanResult[Nothing] = {
   /* Assert.hasText(key)
    Assert.notNull(params)*/

    Assert.hasText(key);
    Assert.notNull(params);

    try {
      final ScanResult<Tuple> res = cluster.zscan(key, String.valueOf(cursor), params);
      final List<Tuple> tuples = res.getResult();
      if (CollectionUtils.isEmpty(tuples)) {
        return new ScanResult<>(res.getStringCursor(), Collections.emptyList());
      }

      final List<Entry<String, Double>> newTuples = Lists.newArrayList();
      tuples.forEach(tuple -> newTuples.add(new AbstractMap.SimpleEntry<>(tuple.getElement(), tuple.getScore())));
      return new ScanResult<>(res.getStringCursor(), newTuples);
    } catch (final Throwable e) {
      throw new RedisClientException(e.getMessage(), e);
    }
}*/
/*
@Test
public void zscanTest() {
    final String key = "zscan.dev";
    final String prefix = "zscan.dev-";
    try {
        final Map<Object, Double> map = Maps.newHashMap();
        for (int idx = 0; idx < 1000; idx++) {
            map.put(prefix + idx, Double.valueOf(idx));
        }

        redisClient.zadd(key, map);
        final AtomicLong cursor = new AtomicLong(-1);
        final ScanParams params = new ScanParams().count(10);
        while (cursor.get() == -1 || cursor.get() > 0) {
            if (cursor.get() == -1) {
                cursor.set(0);
            }

            final ScanResult<Entry<String, Double>> res = redisClient.zscan(key, cursor.get(), params);
            cursor.set(Long.valueOf(res.getStringCursor()));
        }
    } finally {
        redisClient.del(key);
    }
}
 */
