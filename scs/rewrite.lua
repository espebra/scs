local common = require "common"
local Request = require "request"
local timer = require "timer"
local cjson = require 'cjson'

local function rewrite_request(r)
    local object = r.object
    local bucket = r.bucket
    local dir = r.dir
    local object_base64 = r.object_base64
    local dir = common.get_storage_directory()

    if object and bucket and object_base64 then
        -- See if the object exists locally, and rewrite if it does
        local versions = common.get_local_object_versions(bucket, object)
        if #versions > 0 then
            -- Print the checksum as a response header
            ngx.header['X-Md5'] = versions[1].md5
            local depth = common.get_directory_depth(object)

            local uri = "/" .. bucket .. "/" .. depth .. "/" .. object_base64 .. "/" .. versions[1].version .. "-" .. versions[1].md5 .. ".data"
            ngx.log(ngx.INFO,"Found " .. bucket .. "/" .. object .. " in local file system. Rewriting URI " .. ngx.var.uri .. " to " .. uri)
            ngx.req.set_uri(uri, true)
        end
    end

    -- None found
end

-- Read the request
local r = Request()

-- Start periodic batch jobs here
timer.initiate_periodic_health_checks(10)

-- Return 200 to the status check
if r.status and r.internal then
    ngx.exit(ngx.HTTP_OK)
end

local method = r.method
if method == "GET" or method == "HEAD" then
    -- Only if a object name is set
    if r.object and not r.meta then
        rewrite_request(r)
    end
end

