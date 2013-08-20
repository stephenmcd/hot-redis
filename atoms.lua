
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
    local delimiter = table.remove(ARGV, 1)
    for _, v in pairs(ARGV) do
        if v ~= delimiter then
            redis.call('SADD', temp_key, v)
        else
            redis.call('SDIFFSTORE', KEYS[1], KEYS[1], temp_key)
            redis.call('DEL', temp_key)
        end
    end
end

function set_symmetric_difference()

    local action = table.remove(ARGV, 1)
    local other_key = ARGV[1]
    local temp_key1 = KEYS[1] .. 'set_symmetric_difference_temp1'
    local temp_key2 = KEYS[1] .. 'set_symmetric_difference_temp2'
    local result = nil

    if action == 'create' then
        other_key = KEYS[1] .. 'set_symmetric_difference_create'
        redis.call('SADD', other_key, unpack(ARGV))
    end

    redis.call('SDIFFSTORE', temp_key1, KEYS[1], other_key)
    redis.call('SDIFFSTORE', temp_key2, other_key, KEYS[1])

    if action == 'update' then
        redis.call('SUNIONSTORE', KEYS[1], temp_key1, temp_key2)
    else
        result = redis.call('SUNION', temp_key1, temp_key2)
        if action == 'create' then
            redis.call('DEL', other_key)
        end
    end

    redis.call('DEL', temp_key1)
    redis.call('DEL', temp_key2)
    return result

end

function string_multiply()
    local s = redis.call('GET', KEYS[1])
    redis.call('SET', KEYS[1], string.rep(s, tonumber(ARGV[1])))
end

function string_setitem()
    local s = redis.call('GET', KEYS[1])
    local start = tonumber(ARGV[1])
    local stop = tonumber(ARGV[2])
    s = string.sub(s, 1, start) .. ARGV[3] .. string.sub(s, stop + 1)
    redis.call('SET', KEYS[1], s)
end

function number_multiply()
    local n = tonumber(redis.call('GET', KEYS[1])) * tonumber(ARGV[1])
    redis.call('SET', KEYS[1], n)
end

function number_divide()
    local n = tonumber(redis.call('GET', KEYS[1])) / tonumber(ARGV[1])
    redis.call('SET', KEYS[1], n)
end

function number_floordiv()
    local n = math.floor(tonumber(redis.call('GET', KEYS[1])) / tonumber(ARGV[1]))
    redis.call('SET', KEYS[1], n)
end

function number_mod()
    local n = tonumber(redis.call('GET', KEYS[1])) % tonumber(ARGV[1])
    redis.call('SET', KEYS[1], n)
end

function number_pow()
    local n = tonumber(redis.call('GET', KEYS[1])) ^ tonumber(ARGV[1])
    redis.call('SET', KEYS[1], n)
end

function number_and()
    local n = bit_and(tonumber(redis.call('GET', KEYS[1])), tonumber(ARGV[1]))
    redis.call('SET', KEYS[1], n)
end

function number_or()
    local n = bit_or(tonumber(redis.call('GET', KEYS[1])), tonumber(ARGV[1]))
    redis.call('SET', KEYS[1], n)
end

function number_xor()
    local n = bit_xor(tonumber(redis.call('GET', KEYS[1])), tonumber(ARGV[1]))
    redis.call('SET', KEYS[1], n)
end

function number_lshift()
    local n = bit_lshift(tonumber(redis.call('GET', KEYS[1])), tonumber(ARGV[1]))
    redis.call('SET', KEYS[1], n)
end

function number_rshift()
    local n = bit_rshift(tonumber(redis.call('GET', KEYS[1])), tonumber(ARGV[1]))
    redis.call('SET', KEYS[1], n)
end

function queue_put()
    local size = redis.call('LLEN', KEYS[1])
    local maxsize = tonumber(ARGV[2])
    if size >= maxsize then
        return 0
    end
    redis.call('RPUSH', KEYS[1], ARGV[1])
    return 1
end

function multiset_intersection_update()
    local keys_values = redis.call('HGETALL', KEYS[1])
    local all = {}
    for i = 1, #keys_values, 2 do
        all[keys_values[i]] = keys_values[i+1]
    end
    redis.call('DEL', KEYS[1])
    for i = 1, #ARGV, 2 do
        local current = tonumber(all[ARGV[i]])
        local new = tonumber(ARGV[i+1])
        if new > 0 and current then
            redis.call('HSET', KEYS[1], ARGV[i], math.min(new, current))
        end
    end
end

function multiset_union_update()
    for i = 1, #ARGV, 2 do
        local current = tonumber(redis.call('HGET', KEYS[1], ARGV[i]))
        local new = tonumber(ARGV[i+1])
        if new > 0 and (not current or new > current) then
            redis.call('HSET', KEYS[1], ARGV[i], new)
        end
    end
end
