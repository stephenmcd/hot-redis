
function list_pop()
    local l = redis.call('LRANGE', KEYS[1], 0, -1)
    local i = tonumber(ARGV[1]) + 1
    local v = table.remove(l, i)
    redis.call('DEL', KEYS[1])
    redis.call('RPUSH', KEYS[1], unpack(l))
    return v
end

function list_insert()
    local l = redis.call('LRANGE', KEYS[1], 0, -1)
    local i = tonumber(ARGV[1]) + 1
    table.insert(l, i, ARGV[2])
    redis.call('DEL', KEYS[1])
    redis.call('RPUSH', KEYS[1], unpack(l))
end

function list_reverse()
    local l = redis.call('LRANGE', KEYS[1], 0, -1)
    redis.call('DEL', KEYS[1])
    redis.call('LPUSH', KEYS[1], unpack(l))
end

function list_multiply()
    local l = redis.call('LRANGE', KEYS[1], 0, -1)
    redis.call('DEL', KEYS[1])
    if l[1] then
        local i = tonumber(ARGV[1])
        while i > 0 do
            i = i - 1
            redis.call('RPUSH', KEYS[1], unpack(l))
        end
    end
end

function set_intersection_update()
    local temp_key = KEYS[1] .. 'set_intersection_update'
    redis.call('SADD', temp_key, unpack(ARGV))
    redis.call('SINTERSTORE', KEYS[1], KEYS[1], temp_key)
    redis.call('DEL', temp_key)
end

function set_difference_update()
    local temp_key = KEYS[1] .. 'set_difference_update'
    redis.call('SADD', temp_key, unpack(ARGV))
    redis.call('SDIFFSTORE', KEYS[1], KEYS[1], temp_key)
    redis.call('DEL', temp_key)
end
