local M = {}
local ngx = require "ngx"
local cjson = require "cjson"

function M.set_cache(key, t)
    local cache = ngx.shared.cache
    if not cache then
        return false
    end

    local value = cjson.encode(t)
    local succ, err, forcible = cache:set(key, value)
    if succ then
        ngx.log(ngx.INFO,"Cache: Set '" .. key .. "' successfully, " .. #value .. " bytes")
    else
        if err then
            ngx.log(ngx.ERR,"Cache: Unable to set '" .. key .. "', " .. #value .. " bytes. Error: " .. err)
        else
            ngx.log(ngx.ERR,"Cache: Unable to set '" .. key .. "', " .. #value .. " bytes")
        end
    end
    return succ
end

function M.get_cache(key)
    local cache = ngx.shared.cache
    if cache then
        local value, flags = cache:get(key)
        if value then
            ngx.log(ngx.DEBUG,"Cache: Read '" .. key .. "', " .. #value .. " bytes")
            local t = cjson.decode(value)
            if t then
                return t
            end
        end
    end
    ngx.log(ngx.DEBUG,"Cache: Read '" .. key .. "' failed. Cache entry does not exist.")
    return false
end

return M
