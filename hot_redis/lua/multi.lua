
function rank_lists_by_length()
    local ranker_key = "__tmp__.hot_redis.rank_lists_by_length." .. KEYS[1]
    for _, key in pairs(KEYS) do
        redis.call('ZADD',
            ranker_key,
            redis.call('LLEN', key),
            key)
    end
    local result = redis.call('ZREVRANGE', ranker_key, ARGV[1], ARGV[2],
        'WITHSCORES')
    redis.call('DEL', ranker_key)
    return result
end

function rank_sets_by_cardinality()
    local ranker_key = "__tmp__.hot_redis.rank_sets_by_cardinality." .. KEYS[1]
    for _, key in pairs(KEYS) do
        redis.call('ZADD',
            ranker_key,
            redis.call('SCARD', key),
            key)
    end
    local result = redis.call('ZREVRANGE', ranker_key, ARGV[1], ARGV[2],
        'WITHSCORES')
    redis.call('DEL', ranker_key)
    return result
end