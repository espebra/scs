local M = {}
local ngx = require "ngx"
local cjson = require "cjson"

function M.set_cache(key, t)
    local cache = ngx.shared.cache
    if not cache then
        return false
    end

    local value = false

    if type(t) == 'table' then
        value = cjson.encode(t)
    elseif type(t) == 'boolean' then
        value = t
    end

    local succ, err, forcible = cache:set(key, value)
    if succ then
        ngx.log(ngx.INFO,"Cache: Set '" .. key .. "' successfully, type " .. type(value))
    else
        if err then
            ngx.log(ngx.ERR,"Cache: Unable to set '" .. key .. "', type " .. type(value) .. ". Error: " .. err)
        else
            ngx.log(ngx.ERR,"Cache: Unable to set '" .. key .. "', type " .. type(value))
        end
    end
    return succ
end

function M.get_cache(key)
    local cache = ngx.shared.cache
    if cache then
        local value, flags = cache:get(key)
        if value then
            ngx.log(ngx.DEBUG,"Cache: Read '" .. key .. "', type " .. type(value))
            if type(value) == 'boolean' then
                return value
            else
                local t = cjson.decode(value)
                if t then
                    return t
                end
            end
        end
        return false
    end
    ngx.log(ngx.DEBUG,"Cache: Read '" .. key .. "' failed. Cache entry does not exist.")
    return nil
end

return M
