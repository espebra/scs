local class = require "kidclass"
local cjson = require "cjson"
local cache = require "cache"
local ngx = require "ngx"
local Configuration = class.new();

---------------
-- Private API
---------------
--local function get_directory_depth(object)
--    local md5 = ngx.md5(object)
--    local dir = false
--    if md5 then
--        local m, err = ngx.re.match(md5, "^(..)(..)",'j')
--        if m then
--            if #m == 2 then
--                dir = m[1] .. "/" .. m[2]
--            end
--        end
--    end
--    return dir
--end
--
--local function verify_bucket(bucket)
--    -- Must not be false
--    if not bucket then
--        --ngx.log(ngx.WARN,"Bucket name is not set")
--        return false
--    end
--
--    -- Must not be less than 3 characters
--    if #bucket < 3 then
--        --ngx.log(ngx.WARN,"Bucket name is too short")
--        return false
--    end
--
--    -- Must not be more than 63 characters
--    if #bucket > 63 then
--        --ngx.log(ngx.WARN,"Bucket name is too long")
--        return false
--    end
--
--    -- Must contain only allowed characters
--    if not ngx.re.match(bucket, '^[a-z0-9-]+$','j') then
--        --ngx.log(ngx.WARN,"Bucket name contains illegal characters")
--        return false
--    end
--
--    -- Must not start with -
--    if ngx.re.match(bucket, '^-','j') then
--        --ngx.log(ngx.WARN,"Bucket name starts with -")
--        return false
--    end
--
--    -- Must not end with -
--    if ngx.re.match(bucket, '-$','j') then
--        --ngx.log(ngx.WARN,"Bucket name ends with -")
--        return false
--    end
--
--    return true
--end
--
--local function _is_internal(useragent)
--    if useragent == "scs internal" then
--        return true
--    else
--        return false
--    end
--end

local function read_file(path)
    local f = nil
    local content = false

    local key = "file " .. path
    local result = cache.get_cache(key)
    if result then
        return result
    end

    f = io.open(path, "r")
    if f then
        content = f:read("*all")
        f:close()
        local value = cjson.decode(content)
        if value then
            cache.set_cache(key, value)
            return value
        end
    end

    return false
end

---------------
-- Public API
---------------
function Configuration.Constructor(self)
    local c = read_file("/etc/scs/common.conf")
    local l = read_file("/etc/scs/local.conf")
    local h = read_file("/etc/scs/hosts.conf")

    if c then
        self.replica_sites = c.replica_sites
        self.replicas_per_site = c.replicas_per_site
    end

    if l then
        self.objects = l.objects
        self.queue = l.queue
    end

    if h then
        self.hosts = h
    end
end

return Configuration
